package io.github.wechaty.io.github.wechaty.memorycard.backend

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import io.github.wechaty.memorycard.*
import io.github.wechaty.utils.JsonUtils
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import java.io.File

class StorageS3(val name: String, var options: StorageBackendOptions) : StorageBackend(name,options) {

    private lateinit var s3: AmazonS3

    init {

        log.info("StorageS3, constructor()")
        options.type = "s3"
        options = options as StorageS3Options
        val basicAWSCredentials = BasicAWSCredentials((options as StorageS3Options).accessKeyId, (options as StorageS3Options).secretAccessKey)
        this.s3 = AmazonS3ClientBuilder.standard().withCredentials(AWSStaticCredentialsProvider(basicAWSCredentials))
            .withRegion((options as StorageS3Options).region).build()

    }

    override fun save(payload: MemoryCardPayload) {
        log.info("StorageS3, save()")
        val options = this.options as StorageS3Options

        this.s3.putObject(JsonUtils.write(payload), options.bucket, this.name)

    }

    override fun load(): MemoryCardPayload {
        log.info("StorageS3, load()")

        val options = this.options as StorageS3Options
        val result = this.s3.getObject(options.bucket, this.name)
        if (result==null || result.objectContent == null) {
            return MemoryCardPayload()
        }
        // 这里还有问题
        return MemoryCardPayload()
    }

    override fun destory() {
        log.info("StorageS3, destory()")

        val options = this.options as StorageS3Options
        this.s3.deleteObject(options.bucket, this.name)
    }

    override fun toString(): String {
        return "${this.name}<${this.name}>"
    }


    companion object {
        private val log = LoggerFactory.getLogger(StorageS3::class.java)
    }

}



fun main(){

    StorageFile("test", StorageFileOptions())

}
