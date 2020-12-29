package io.github.wechaty.user

import io.github.wechaty.Accessory
import io.github.wechaty.Wechaty
import io.github.wechaty.schemas.FriendshipPayload
import io.github.wechaty.schemas.FriendshipType
import io.github.wechaty.utils.JsonUtils
import org.slf4j.LoggerFactory

class Friendship (wechaty: Wechaty,val id:String):Accessory(wechaty){


    private var payload:FriendshipPayload? = null

    fun isReady():Boolean{
        return  payload != null
    }

    fun ready(){
        if(isReady()){
            return
        }
        this.payload = wechaty.getPuppet().friendshipPayload(id!!).get()
        contact().ready()

    }

    fun contact():Contact{
        if(payload == null){
            throw Exception("no payload")
        }
        return wechaty.contactManager.load(payload!!.contactId!!)
    }

    fun accept(){
        if(payload == null){
            throw Exception("no payload")
        }

        if(payload!!.type != FriendshipType.Receive){
            throw Exception("accept() need type to be FriendshipType.Receive, but it got a ${payload!!.type}")
        }
        wechaty.getPuppet().friendshipAccept(this.id).get()
        val contact = contact()
        contact.ready()
        contact.sync()
    }

    fun hello():String{
        if(payload==null){
            throw Exception("no payload")
        }
        return this.payload?.hello ?: "";
    }

    fun type():FriendshipType{
        return this.payload?.type ?:FriendshipType.Unknown
    }

    fun toJson():String{
        if(payload==null){
            throw Exception("no payload")
        }
        return JsonUtils.write(payload!!);
    }

    fun getType():FriendshipType{
        return payload?.type ?: throw Exception("no payload")
    }

    companion object{
        private val log = LoggerFactory.getLogger(Friendship::class.java)
    }

}

