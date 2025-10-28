/*
 * Copyright (C) 2024-2025 OpenAni and contributors.
 *
 * 此源代码的使用受 GNU AFFERO GENERAL PUBLIC LICENSE version 3 许可证的约束, 可以在以下链接找到该许可证.
 * Use of this source code is governed by the GNU AGPLv3 license, which can be found at the following link.
 *
 * https://github.com/open-ani/ani/blob/main/LICENSE
 */

package me.him188.ani.datasources.jellyfin

import io.ktor.client.call.body
import io.ktor.client.request.HttpRequestBuilder
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.parameter
import io.ktor.http.HttpHeaders
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.Serializable
import kotlinx.datetime.*
import me.him188.ani.datasources.api.DefaultMedia
import me.him188.ani.datasources.api.EpisodeSort
import me.him188.ani.datasources.api.MediaExtraFiles
import me.him188.ani.datasources.api.MediaProperties
import me.him188.ani.datasources.api.Subtitle
import me.him188.ani.datasources.api.SubtitleKind
import me.him188.ani.datasources.api.paging.SinglePagePagedSource
import me.him188.ani.datasources.api.paging.SizedSource
import me.him188.ani.datasources.api.source.ConnectionStatus
import me.him188.ani.datasources.api.source.HttpMediaSource
import me.him188.ani.datasources.api.source.MatchKind
import me.him188.ani.datasources.api.source.MediaFetchRequest
import me.him188.ani.datasources.api.source.MediaMatch
import me.him188.ani.datasources.api.source.MediaSourceKind
import me.him188.ani.datasources.api.source.MediaSourceLocation
import me.him188.ani.datasources.api.source.matches
import me.him188.ani.datasources.api.topic.EpisodeRange
import me.him188.ani.datasources.api.topic.FileSize
import me.him188.ani.datasources.api.topic.ResourceLocation
import me.him188.ani.utils.ktor.ScopedHttpClient
import me.him188.ani.utils.logging.error
import me.him188.ani.utils.logging.logger
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Duration.Companion.minutes

abstract class BaseJellyfinMediaSource(
    private val client: ScopedHttpClient,
) : HttpMediaSource() {
    companion object {
        private val logger = logger<BaseJellyfinMediaSource>()
        private const val MAX_RETRIES = 3
        private val CACHE_EXPIRY = 5.minutes
    }
    
    abstract val baseUrl: String
    abstract val userId: String
    abstract val apiKey: String
    
    // 线程安全的内存缓存
    private val searchCache = mutableMapOf<String, Pair<Long, SearchResponse>>()
    private val cacheMutex = Mutex()
    
    // 简单的时间戳计数器，用于替代System.currentTimeMillis()
    private var timeCounter = 0L
    
    // 获取当前时间的函数，兼容多平台
    private fun getCurrentTime(): Long {
        return ++timeCounter
    }
    
    private fun getCacheKey(
        subjectName: String? = null,
        parentId: String? = null,
        includeItemTypes: Set<String> = emptySet()
    ): String {
        // 使用哈希码避免缓存键过长
        val typesHash = if (includeItemTypes.isNotEmpty()) {
            includeItemTypes.joinToString(",").hashCode().toString()
        } else {
            ""
        }
        return "${subjectName}_${parentId}_$typesHash"
    }
    
    private suspend fun getCachedResponse(cacheKey: String): SearchResponse? {
        return cacheMutex.withLock {
            val (timestamp, response) = searchCache[cacheKey] ?: return@withLock null
            return@withLock if (getCurrentTime() - timestamp < CACHE_EXPIRY.inWholeMilliseconds) {
                response
            } else {
                searchCache.remove(cacheKey)
                null
            }
        }
    }
    
    private suspend fun cacheResponse(cacheKey: String, response: SearchResponse) {
        cacheMutex.withLock {
            searchCache[cacheKey] = Pair(getCurrentTime(), response)
        }
    }

    override suspend fun checkConnection(): ConnectionStatus {
        try {
            doSearch("AA测试BB")
            return ConnectionStatus.SUCCESS
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            return ConnectionStatus.FAILED
        }
    }

    override suspend fun fetch(query: MediaFetchRequest): SizedSource<MediaMatch> {
        return SinglePagePagedSource {
            query.subjectNames
                .asFlow()
                .flatMapConcat { subjectName ->
                    try {
                        // 首先尝试精确搜索，使用完整的标题
                        val exactResults = doSearch(
                            subjectName = subjectName,
                            recursive = false, // 不递归搜索，只搜索顶级
                            includeItemTypes = setOf("Episode", "Movie", "Series")
                        ).Items
                        
                        // 如果有精确匹配的Episode或Movie，直接返回
                        val directEpisodes = exactResults.filter { it.Type == "Episode" || it.Type == "Movie" }
                        if (directEpisodes.isNotEmpty()) {
                            return@flatMapConcat flowOf(directEpisodes)
                        }
                        
                        // 如果有精确匹配的Series，获取其所有内容
                        val exactSeries = exactResults.filter { it.Type == "Series" }
                        if (exactSeries.isNotEmpty()) {
                            return@flatMapConcat fetchAllSeriesContent(exactSeries)
                        }
                        
                        // 如果没有精确匹配，尝试模糊搜索Episode和Movie
                        val fuzzyResults = doSearch(
                            subjectName = subjectName,
                            recursive = false,
                            includeItemTypes = setOf("Episode", "Movie")
                        ).Items
                        
                        // 如果模糊搜索有结果，直接返回
                        if (fuzzyResults.isNotEmpty()) {
                            return@flatMapConcat flowOf(fuzzyResults)
                        }
                        
                        // 最后尝试搜索Series并获取其所有内容
                        val seriesResults = doSearch(
                            subjectName = subjectName,
                            recursive = false,
                            includeItemTypes = setOf("Series")
                        ).Items
                        
                        fetchAllSeriesContent(seriesResults)
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        // 记录错误并返回空流，避免一个搜索失败影响其他搜索
                        logger.error("搜索主题 '$subjectName' 时发生错误", e)
                        emptyFlow()
                    }
                }
                .filter { (it as Item).Type == "Episode" || (it as Item).Type == "Movie" }
                .toList()
                .distinctBy { item: Any -> (item as Item).Id }
                .mapNotNull { item: Any ->
                    try {
                        createMediaMatch(item as Item, query)
                    } catch (e: Exception) {
                        logger.error("创建MediaMatch对象失败: item.Id=${(item as? Item)?.Id}, item.Type=${(item as? Item)?.Type}", e)
                        null
                    }
                }
                .filter { it.matches(query) != false }
                .asFlow()
        }
    }
    
    /**
     * 获取所有Series的内容（包括所有季和集）
     * 使用并行处理提高效率
     */
    private suspend fun fetchAllSeriesContent(seriesList: List<Item>) = seriesList.asFlow()
        .flatMapMerge { series ->
            try {
                // 获取该系列的所有季
                val seasons = doSearch(
                    parentId = series.Id,
                    includeItemTypes = setOf("Season")
                ).Items
                
                // 如果没有季，直接返回空流
                if (seasons.isEmpty()) {
                    logger.warn("系列 ${series.Name} (ID: ${series.Id}) 没有任何季", null)
                    return@flatMapMerge emptyFlow<Item>()
                }
                
                seasons.asFlow()
                    .flatMapMerge { season ->
                        try {
                            // 获取该季的所有集
                            val episodes = doSearch(
                                parentId = season.Id,
                                includeItemTypes = setOf("Episode")
                            ).Items
                            
                            // 如果没有集，记录警告但继续处理其他季
                            if (episodes.isEmpty()) {
                                logger.warn("季 ${season.Name} (ID: ${season.Id}) 没有任何集", null)
                            }
                            
                            episodes.asFlow()
                        } catch (e: Exception) {
                            logger.error("获取季 ${season.Id} 的集失败", e)
                            emptyFlow()
                        }
                    }
            } catch (e: Exception) {
                logger.error("获取系列 ${series.Id} 的季失败", e)
                emptyFlow()
            }
        }
    
    /**
     * 根据Item创建MediaMatch对象
     */
    private suspend fun createMediaMatch(item: Item, query: MediaFetchRequest): MediaMatch? {
        try {
            val (originalTitle, episodeRange) = when (item.Type) {
                "Episode" -> {
                    val indexNumber = item.IndexNumber ?: run {
                        logger.warn("Episode缺少IndexNumber: ${item.Name} (ID: ${item.Id})", null)
                        return null
                    }
                    Pair(
                        "$indexNumber ${item.Name}",
                        EpisodeRange.single(EpisodeSort(indexNumber)),
                    )
                }

                "Movie" -> Pair(
                    item.Name,
                    EpisodeRange.unknownSeason(),
                )

                else -> {
                    logger.warn("不支持的Item类型: ${item.Type} (ID: ${item.Id})", null)
                    return null
                }
            }

            return MediaMatch(
                media = DefaultMedia(
                    mediaId = item.Id,
                    mediaSourceId = mediaSourceId,
                    originalUrl = "$baseUrl/Items/${item.Id}",
                    download = ResourceLocation.HttpStreamingFile(
                        uri = getDownloadUri(item.Id),
                    ),
                    originalTitle = originalTitle,
                    publishedTime = 0,
                    properties = MediaProperties(
                        // Note: 这里我们 fallback 使用请求的名称, 这样可以在缺少信息时绝对通过后续的过滤, 避免资源被排除. 但这可能会导致有不满足的资源被匹配. 如果未来有问题再考虑. See also #1806.
                        subjectName = item.SeriesName?.takeIf { it.isNotBlank() } 
                            ?: item.SeasonName?.takeIf { it.isNotBlank() } 
                            ?: query.subjectNameCN,
                        episodeName = item.Name,
                        subtitleLanguageIds = listOf("CHS"),
                        resolution = "1080P",
                        alliance = mediaSourceId,
                        size = FileSize.Unspecified,
                        subtitleKind = SubtitleKind.EXTERNAL_PROVIDED,
                    ),
                    extraFiles = MediaExtraFiles(
                        subtitles = try {
                            getSubtitles(item.Id, item.MediaStreams)
                        } catch (e: Exception) {
                            logger.error("获取字幕失败: item.Id=${item.Id}", e)
                            emptyList()
                        }
                    ),
                    episodeRange = episodeRange,
                    location = MediaSourceLocation.Lan,
                    kind = MediaSourceKind.WEB,
                ),
                kind = MatchKind.FUZZY,
            )
        } catch (e: Exception) {
            logger.error("创建MediaMatch对象时发生未预期的错误: item.Id=${item.Id}, item.Type=${item.Type}", e)
            return null
        }
    }

    protected abstract fun getDownloadUri(itemId: String): String

    private fun getSubtitles(itemId: String, mediaStreams: List<MediaStream>): List<Subtitle> {
        return mediaStreams
            .filter { it.Type == "Subtitle" && it.IsTextSubtitleStream && it.IsExternal && it.Codec != null }
            .map { stream ->
                Subtitle(
                    uri = getSubtitleUri(itemId, stream.Index, stream.Codec!!),
                    language = stream.Language,
                    mimeType = when (stream.Codec.lowercase()) {
                        "ass" -> "text/x-ass"
                        else -> "application/octet-stream"  // 默认二进制流
                    },
                    label = stream.Title,
                )
            }
    }

    private fun getSubtitleUri(itemId: String, index: Int, codec: String): String {
        return "$baseUrl/Videos/$itemId/$itemId/Subtitles/$index/0/Stream.$codec"
    }


    private suspend fun doSearch(
        subjectName: String? = null,
        parentId: String? = null,
        recursive: Boolean = false,
        includeItemTypes: Set<String> = emptySet(),
    ): SearchResponse {
        val cacheKey = getCacheKey(subjectName, parentId, includeItemTypes)
        
        // 先检查缓存
        getCachedResponse(cacheKey)?.let { cachedResponse ->
            logger.info("使用缓存响应: $cacheKey", null)
            return cachedResponse
        }
        
        // 缓存未命中，执行实际搜索
        var lastException: Exception? = null
        var response: SearchResponse? = null
        try {
            repeat(MAX_RETRIES) { attempt ->
                try {
                    response = client.use {
                        get("$baseUrl/Items") {
                            configureAuthorizationHeaders()
                            parameter("userId", userId)
                            parameter("enableImages", false)
                            parameter("recursive", recursive)
                            if (subjectName != null) {
                                parameter("searchTerm", subjectName)
                            }
                            parameter("fields", "CanDownload")
                            parameter("fields", "MediaStreams")
                            if (parentId != null) {
                                parameter("parentId", parentId)
                            }
                            if (includeItemTypes.isNotEmpty()) {
                                parameter("includeItemTypes", includeItemTypes.joinToString(","))
                            }
                        }.body<SearchResponse>()
                    }
                    return@repeat // 成功获取结果，退出重试循环
                } catch (e: CancellationException) {
                    throw e // 取消异常直接抛出，不重试
                } catch (e: Exception) {
                    lastException = e
                    if (attempt < MAX_RETRIES - 1) {
                        logger.error("搜索失败: subjectName=$subjectName, parentId=$parentId, includeItemTypes=$includeItemTypes", e)
                        logger.info("搜索重试 (尝试 ${attempt + 1}/$MAX_RETRIES): ${e.message}", null)
                        delay(1.seconds)
                    }
                }
            }
            if (response == null) {
                throw lastException ?: Exception("未知错误")
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            logger.error("搜索失败: subjectName=$subjectName, parentId=$parentId, includeItemTypes=$includeItemTypes", e)
            throw e
        }
        
        // 缓存响应
        cacheResponse(cacheKey, response)
        return response
    }

    private fun HttpRequestBuilder.configureAuthorizationHeaders() {
        header(
            HttpHeaders.Authorization,
            "MediaBrowser Token=\"$apiKey\"",
        )
    }
}

@Serializable
private class SearchResponse(
    val Items: List<Item> = emptyList(),
)

@Serializable
@Suppress("PropertyName")
private data class MediaStream(
    val Title: String? = null, // 除了字幕以外其他可能没有
    val Language: String? = null, // 字幕语言代码，如 chs
    val Type: String,
    val Codec: String? = null, // 除了字幕以外其他可能没有
    val Index: Int,
    val IsExternal: Boolean, // 是否为外挂字幕
    val IsTextSubtitleStream: Boolean, // 是否可下载
)

@Serializable
@Suppress("PropertyName")
private data class Item(
    val Name: String,
    val SeasonName: String? = null,
    val SeriesName: String? = null,
    val Id: String,
    val OriginalTitle: String? = null, // 日文
    val IndexNumber: Int? = null,
    val ParentIndexNumber: Int? = null,
    val Type: String, // "Episode", "Series", ...
    val CanDownload: Boolean = false,
    val MediaStreams: List<MediaStream> = emptyList(),
)
