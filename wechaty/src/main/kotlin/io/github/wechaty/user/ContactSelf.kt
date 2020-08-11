package io.github.wechaty.user

import io.github.wechaty.Wechaty
import io.github.wechaty.filebox.FileBox
import io.github.wechaty.utils.QrcodeUtils.Companion.guardQrCodeValue
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future

class ContactSelf(wechaty: Wechaty, id: String) : Contact(wechaty, id) {

    fun avatar(fileBox: FileBox) {
        puppet.setContactAvatar(super.id, fileBox)
    }

    fun getAvatar(fileBox: FileBox) {
        puppet.setContactAvatar(super.id, fileBox)
    }

    fun setName(name:String){
        puppet.contactSelfName(name).get()
        sync()
    }

    fun signature(signature:String){

        var puppetId:String? = puppet.selfId()

        let{
            puppetId != null
        }.run {
            puppet.contactSelfSignature(signature).get()
            sync()
        }
    }
    fun setSignature(signature:String) {

        var puppetId:String? = puppet.selfId()

        let{
            puppetId != null
        }.run {
            puppet.contactSelfSignature(signature).get()
            sync()
        }
    }

    fun qrcode(): Future<String> {
        log.info("Contact, qrcode()")

        val puppetId: String = try {
            this.puppet.selfId().toString()
        }
        catch (e: Exception) {
            throw Exception("Can not get qrcode, user might be either not logged in or already logged out")
        }

        if (this.id !== puppetId) {
            throw Exception("only can get qrcode for the login userself")
        }

        return CompletableFuture.supplyAsync {
            val qrcodeValue = this.puppet.contactSelfQRCode().get()
            guardQrCodeValue(qrcodeValue)
        }
    }
    fun getQrcode(): Future<String> {
        log.info("Contact, qrcode()")

        val puppetId: String = try {
            this.puppet.selfId().toString()
        }
        catch (e: Exception) {
            throw Exception("Can not get qrcode, user might be either not logged in or already logged out")
        }

        if (this.id !== puppetId) {
            throw Exception("only can get qrcode for the login userself")
        }

        return CompletableFuture.supplyAsync {
            val qrcodeValue = this.puppet.contactSelfQRCode().get()
            guardQrCodeValue(qrcodeValue)
        }
    }

    fun name(name: String?): String? {
        if (name == null) {
            return super.name()
        }
        val puppetId = try {
            this.puppet.selfId()
        } catch (e: Exception) {
            throw Exception("Can not get qrcode, user might be either not logged in or already logged out")
        }
        if (this.id !== puppetId) {
            throw Exception("only can get qrcode for the login userself")
        }
        this.puppet.contactSelfName(name)
        return null
    }

    fun getName(name: String?): String? {
        if (name == null) {
            return super.name()
        }
        val puppetId = try {
            this.puppet.selfId()
        } catch (e: Exception) {
            throw Exception("Can not get qrcode, user might be either not logged in or already logged out")
        }
        if (this.id !== puppetId) {
            throw Exception("only can get qrcode for the login userself")
        }
        this.puppet.contactSelfName(name)
        return null
    }

    companion object {
        private val log = LoggerFactory.getLogger(ContactSelf::class.java)
    }
}
