package me.func

import io.minio.DownloadObjectArgs
import io.minio.ListObjectsArgs
import io.minio.MinioClient
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import java.io.File
import java.util.concurrent.atomic.AtomicLong

val ACCESS_KEY: String = System.getenv("ACCESS_KEY")
val SECRET_KEY: String = System.getenv("SECRET_KEY")
val BUCKET: String = System.getenv("BUCKET")
val ENDPOINT = System.getenv("ENDPOINT") ?: "https://ams1.vultrobjects.com"

val OUTPUT_DIR = System.getenv("OUTPUT_DIR") ?: "output"
val DOWNLOAD_LIMIT = System.getenv("DOWNLOAD_LIMIT").toLongOrNull() ?: 100L

fun main() {
    val bucket = BUCKET
    val client: MinioClient = MinioClient.builder()
        .endpoint(ENDPOINT)
        .credentials(
            ACCESS_KEY,
            SECRET_KEY
        ).build()

    val outputDir = createDir(OUTPUT_DIR)

    // получение списка файлов
    val objectList = client.listObjects(
        ListObjectsArgs.builder()
            .recursive(true)
            .bucket(bucket)
            .build()
    )

    val limit: Long = DOWNLOAD_LIMIT
    val packSize = 50
    val counter = AtomicLong(0)

    // разбиваем по группам
    val totalList = objectList.asSequence()
        .map { it.get().objectName() }
        .chunked(packSize)
        .toList()

    println("Files in storage: " + totalList.size * packSize)

    val scope = CoroutineScope(Dispatchers.IO)

    // параллельно загружаем группы
    totalList.forEach { group ->
        scope.launch {
            if (counter.get() > limit) {
                return@launch
            }

            group.forEach {
                val filePath = "" + outputDir + File.separator + it
                createDir(filePath)

                client.downloadObject(
                    DownloadObjectArgs.builder()
                        .bucket(bucket)
                        .filename(filePath)
                        .overwrite(true)
                        .`object`(it)
                        .build()
                )
                println("Downloaded ${counter.getAndIncrement()} from $limit")
            }
        }
    }

    println("Completed.")
}

fun createDir(dir: String) = File(dir).apply {
    if (!exists()) mkdirs()
}
