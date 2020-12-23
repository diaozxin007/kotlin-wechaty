package io.github.wechaty.grpc

import com.google.protobuf.StringValue
import io.github.wechaty.Puppet
import io.github.wechaty.StateEnum
import io.github.wechaty.filebox.FileBox
import io.github.wechaty.grpc.puppet.*
import io.github.wechaty.io.github.wechaty.schemas.EventEnum
import io.github.wechaty.schemas.*
import io.github.wechaty.utils.JsonUtils
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver
import okhttp3.OkHttpClient
import okhttp3.Request
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.math.NumberUtils
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors.newFixedThreadPool
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import kotlin.system.exitProcess

/**
 * puppet
 * @author zhengxin
 */
class GrpcPuppet(puppetOptions: PuppetOptions) : Puppet(puppetOptions) {

    private var channel: ManagedChannel? = null

    private val CHATIE_ENDPOINT = "https://api.chatie.io/v0/hosties/"

    private val client: OkHttpClient = OkHttpClient()

    private var grpcClient: PuppetGrpc.PuppetBlockingStub? = null
    private var grpcAsyncClient: PuppetGrpc.PuppetStub? = null

//    private var finishLatch:CountDownLatch? = null

    private fun discoverHostieIp(): Pair<String, String> {

        val token = puppetOptions!!.token

        val request: Request = Request.Builder()
            .url(CHATIE_ENDPOINT + token)
            .build()

        client.newCall(request).execute().use { response ->
            val string = response.body!!.string()
            val readValue = JsonUtils.readValue<Map<String, String>>(string)

            val ip = readValue["ip"] ?: error("cannot get ip by token, check token");
            val port = readValue["port"] ?: "8788"

            return Pair(ip, port)
        }
    }


    override fun start(): Future<Void> {

        val future = CompletableFuture<Void>()

        if (state == StateEnum.ON) {
            log.warn("start() is called on a ON puppet. await ready(on) and return.")
            state = StateEnum.ON
            return future
        }
        state = StateEnum.PENDING

        state = try {
            startGrpcClient().get()
            val state = channel!!.getState(true)
            log.debug(state.name)
            val startRequest = Base.StartRequest.newBuilder().build()
            val start = grpcClient!!.start(startRequest)
            startGrpcStream()
            StateEnum.ON
        } catch (e: Exception) {
            log.error("start() rejection:", e)
            StateEnum.OFF
        }

        return CompletableFuture.completedFuture(null)


    }

    override fun stop(): Future<Void> {

        log.debug("stop()")
        if (state == StateEnum.OFF) {
            log.warn("stop() is called on a OFF puppet. await ready(off) and return.")
            return CompletableFuture.completedFuture(null)
        }

        try {
            if (logonoff()) {
                emit(EventEnum.LOGOUT, EventLogoutPayload(getId()!!, "logout"))

                this.setId(null)
            }

            if (channel != null && !channel!!.isShutdown) {
                try {
                    val stopRequest = Base.StopRequest.newBuilder().build()
                    grpcClient!!.stop(stopRequest)
                } catch (e: Exception) {
                    log.error("stop() this.grpcClient.stop() rejection:", e)
                }
            } else {
                log.warn("stop() this.grpcClient not exist")
            }
            stopGrpcClient().get()

        } catch (e: Exception) {
            log.warn("stop() rejection: ", e)
        } finally {
            state = StateEnum.OFF
        }

        return CompletableFuture.completedFuture(null)
    }

    override fun setPuppetName() {
        puppetOptions!!.name = "io.github.wechaty.grpc.GrpcPuppet"
    }

    override fun unref() {

        log.debug("unref")
        super.unref()

    }

    private fun startGrpcClient(): Future<Void> {
        val endPoint = puppetOptions?.endPoint
        val discoverHostieIp: Pair<String, String>
        discoverHostieIp = if (StringUtils.isEmpty(endPoint)) {
            discoverHostieIp()
        } else {
            val split = StringUtils.split(endPoint, ":")
            if (split.size == 1) {
                Pair(split[0], "8788")
            } else {
                Pair(split[0], split[1])
            }

        }

        if (StringUtils.isEmpty(discoverHostieIp.first) || StringUtils.equals(discoverHostieIp.first, "0.0.0.0")) {
            log.error("cannot get ip by token, check token")
            exitProcess(1)
        }
        val newFixedThreadPool = newFixedThreadPool(16)
        channel = ManagedChannelBuilder.forAddress(discoverHostieIp.first, NumberUtils.toInt(discoverHostieIp.second)).usePlaintext().executor(newFixedThreadPool).build()

        grpcClient = PuppetGrpc.newBlockingStub(channel)
        grpcAsyncClient = PuppetGrpc.newStub(channel)
        return CompletableFuture.completedFuture(null)
    }

    private fun startGrpcStream() {
        val streamObserver = object : StreamObserver<Event.EventResponse> {
            override fun onNext(event: Event.EventResponse?) {
                onGrpcStreamEvent(event!!)
            }

            override fun onError(t: Throwable?) {
                log.error("error of grpc", t)
                val payload = EventResetPayload(t?.message ?: "")
                emit(EventEnum.RESET, payload)
            }

            override fun onCompleted() {
                log.warn("grpc client exit")
            }

        }

        val request = Event.EventRequest.newBuilder().build()
        grpcAsyncClient!!.event(request, streamObserver)
    }


    fun stopGrpcClient(): Future<Void> {

        channel!!.shutdown().awaitTermination(5, TimeUnit.SECONDS)
        log.debug("grpc is shutdown")
        return CompletableFuture.completedFuture(null)
    }

    override fun logout(): Future<Void> {

        if (getId() == null) {
            throw Exception("logout before login?")
        }

        try {
            val request = Base.LogoutRequest.newBuilder().build()
            grpcClient?.logout(request)
        } catch (e: Exception) {
            log.error("logout() rejection: %s", e)
        } finally {
            emit(EventEnum.LOGOUT, EventLogoutPayload(getId()!!, "logout"))
        }
        return CompletableFuture.completedFuture(null)
    }

    override fun ding(data: String?) {
        val request = Base.DingRequest.newBuilder()
            .setData(data)
            .build()

        CompletableFuture.runAsync {
            try {
                grpcClient!!.ding(request)
            } catch (e: Exception) {
                log.error("error", e)
            }
        }
    }

    override fun contactSelfName(name: String): Future<Void> {
        val request = Contact.ContactSelfNameRequest.newBuilder()
            .setName(name)
            .build()
        return CompletableFuture.supplyAsync {
            grpcClient!!.contactSelfName(request)
            null
        }
    }

    override fun contactSelfQRCode(): Future<String> {

        val request = Contact.ContactSelfQRCodeRequest.newBuilder().build()

        return CompletableFuture.supplyAsync {
            val contactSelfQRCode = grpcClient!!.contactSelfQRCode(request)
            contactSelfQRCode.qrcode
        }
    }

    override fun contactSelfSignature(signature: String): Future<Void> {

        val request = Contact.ContactSelfSignatureRequest.newBuilder()
            .setSignature(signature)
            .build()

        return CompletableFuture.supplyAsync {
            grpcClient!!.contactSelfSignature(request)
            null
        }
    }

    override fun tagContactAdd(tagId: String, contactId: String): Future<Void> {

        val request = Tag.TagContactAddRequest.newBuilder()
            .setId(tagId)
            .setContactId(contactId)
            .build()

        return CompletableFuture.supplyAsync {
            grpcClient!!.tagContactAdd(request)
            return@supplyAsync null
        }

    }

    override fun tagContactDelete(tagId: String): Future<Void> {

        val request = Tag.TagContactDeleteRequest.newBuilder()
            .setId(tagId)
            .build()
        return CompletableFuture.supplyAsync {
            grpcClient!!.tagContactDelete(request)
            return@supplyAsync null
        }
    }

    override fun tagContactList(contactId: String): Future<List<String>> {

        val stringValue = StringValue.newBuilder()
            .setValue(contactId)
            .build()

        return CompletableFuture.supplyAsync {

            val request = Tag.TagContactListRequest.newBuilder()
                .setContactId(stringValue)
                .build()
            val contactList = grpcClient!!.tagContactList(request)
            contactList.idsList
        }
    }

    override fun tagContactList(): Future<List<String>> {

        return CompletableFuture.supplyAsync {
            val request = Contact.ContactListRequest.newBuilder().build()
            val contactList = grpcClient!!.contactList(request)
            contactList.idsList
        }
    }

    override fun tagContactRemove(tagId: String, contactId: String): Future<Void> {

        val request = Tag.TagContactRemoveRequest.newBuilder()
            .setId(tagId)
            .setContactId(contactId)
            .build()

        return CompletableFuture.supplyAsync {
            grpcClient!!.tagContactRemove(request)
            return@supplyAsync null
        }


    }

    override fun contactAlias(contactId: String): Future<String> {

        return CompletableFuture.supplyAsync {
            val request = Contact.ContactAliasRequest.newBuilder()
                .setId(contactId)
                .build()
            val response = grpcClient!!.contactAlias(request)
            val alias = response.alias
            return@supplyAsync alias.value
        }
    }

    override fun contactAlias(contactId: String, alias: String?): Future<Void> {

        val stringValue = StringValue.newBuilder().setValue(alias).build()
        val request = Contact.ContactAliasRequest.newBuilder()
            .setId(contactId)
            .setAlias(stringValue)
            .build()

        return CompletableFuture.supplyAsync() {
            grpcClient!!.contactAlias(request)
            null
        }

    }

    override fun getContactAvatar(contactId: String): Future<FileBox> {
        val request = Contact.ContactAvatarRequest.newBuilder()
            .setId(contactId)
            .build()

        return CompletableFuture.supplyAsync {
            val response = grpcClient!!.contactAvatar(request)
            val filebox = response.filebox
            return@supplyAsync FileBox.fromJson(filebox.value)
        }

    }

    override fun setContactAvatar(contactId: String, file: FileBox): Future<Void> {

        val toJsonString = file.toJsonString()

        val value = StringValue.newBuilder().setValue(toJsonString)

        val request = Contact.ContactAvatarRequest.newBuilder()
            .setId(contactId)
            .setFilebox(value)
            .build()

        return CompletableFuture.supplyAsync {
            grpcClient!!.contactAvatar(request)
            return@supplyAsync null
        }

    }

    override fun contactList(): Future<List<String>> {

        val request = Contact.ContactListRequest.newBuilder().build()

        return CompletableFuture.supplyAsync {
            val response = grpcClient!!.contactList(request)
            response.idsList
        }
    }

    override fun contactRawPayload(contractId: String): Future<ContactPayload> {

        val request = Contact.ContactPayloadRequest.newBuilder()
            .setId(contractId)
            .build()

        return CompletableFuture.supplyAsync {
            val response = grpcClient!!.contactPayload(request)
            val payload = ContactPayload(response.id)
            payload.address = response.address
            payload.alias = response.alias
            payload.avatar = response.avatar
            payload.city = response.city
            payload.friend = response.friend
            payload.gender = ContactGender.getByCode(response.gender.number)
            payload.name = response.name
            payload.province = response.province
            payload.signature = response.signature
            payload.star = response.star
            payload.type = ContactType.getByCode(response.type.number)
            payload.weixin = response.weixin
            payload
        }

    }

    override fun contactRawPayloadParser(rawPayload: ContactPayload): Future<ContactPayload> {
        return CompletableFuture.completedFuture(rawPayload)
    }

    override fun friendshipAccept(friendshipId: String): Future<Void> {

        val request = Friendship.FriendshipAcceptRequest.newBuilder()
            .setId(friendshipId)
            .build()

        return CompletableFuture.supplyAsync {
            grpcClient!!.friendshipAccept(request)
            return@supplyAsync null
        }
    }

    override fun friendshipAdd(contractId: String, hello: String): Future<Void> {

        val request = Friendship.FriendshipAddRequest.newBuilder()
            .setContactId(contractId)
            .setHello(hello)
            .build()

        return CompletableFuture.supplyAsync {
            grpcClient!!.friendshipAdd(request)
            return@supplyAsync null
        }

    }

    override fun friendshipSearchPhone(phone: String): Future<String?> {

        val request = Friendship.FriendshipSearchPhoneRequest.newBuilder()
            .setPhone(phone)
            .build()

        return CompletableFuture.supplyAsync {
            val response = grpcClient!!.friendshipSearchPhone(request)
            response.contactId.value
        }
    }

    override fun friendshipSearchWeixin(weixin: String): Future<String?> {
        val request = Friendship.FriendshipSearchWeixinRequest.newBuilder()
            .setWeixin(weixin)
            .build()

        return CompletableFuture.supplyAsync {
            val response = grpcClient!!.friendshipSearchWeixin(request)
            response.contactId.value
        }
    }

    override fun friendshipRawPayload(friendshipId: String): Future<FriendshipPayload> {

        val request = Friendship.FriendshipPayloadRequest.newBuilder()
            .setId(friendshipId)
            .build()

        return CompletableFuture.supplyAsync {
            val response = grpcClient!!.friendshipPayload(request)
            val payload = FriendshipPayload()

            payload.scene = FriendshipSceneType.getByCode(response.scene.number)
            payload.stranger = response.stranger
            payload.ticket = response.ticket
            payload.type = FriendshipType.getByCode(response.type.number)
            payload.contactId = response.contactId
            payload.id = response.id

            payload
        }

    }

    override fun friendshipRawPayloadParser(rawPayload: FriendshipPayload): Future<FriendshipPayload> {
        return CompletableFuture.completedFuture(rawPayload)
    }

    override fun messageContact(messageId: String): Future<String> {
        val request = Message.MessageContactRequest.newBuilder()
            .setId(messageId)
            .build()
        return CompletableFuture.supplyAsync {
            val response = grpcClient!!.messageContact(request)
            response.id
        }
    }

    override fun messageFile(messageId: String): Future<FileBox> {

        val request = Message.MessageFileRequest.newBuilder()
            .setId(messageId)
            .build()

        return CompletableFuture.supplyAsync {
            val response = grpcClient!!.messageFile(request)
            val jsonText = response.filebox
            FileBox.fromJson(jsonText)
        }

    }

    override fun messageImage(messageId: String, imageType: ImageType): Future<FileBox> {

        val imageType1 = Message.ImageType.forNumber(imageType.code)
        val request = Message.MessageImageRequest.newBuilder()
            .setId(messageId)
            .setType(imageType1)
            .build()

        return CompletableFuture.supplyAsync {
            val response = grpcClient!!.messageImage(request)
            val filebox = response.filebox
            return@supplyAsync FileBox.fromJson(filebox)
        }

    }

    override fun messageMiniProgram(messageId: String): Future<MiniProgramPayload> {

        val request = Message.MessageMiniProgramRequest.newBuilder()
            .setId(messageId)
            .build()

        return CompletableFuture.supplyAsync {
            val response = grpcClient!!.messageMiniProgram(request)
            val miniProgram = response.miniProgram
            JsonUtils.readValue<MiniProgramPayload>(miniProgram)
        }

    }

    override fun messageUrl(messageId: String): Future<UrlLinkPayload> {

        val request = Message.MessageUrlRequest.newBuilder()
            .setId(messageId)
            .build()

        return CompletableFuture.supplyAsync {

            val response = grpcClient!!.messageUrl(request)
            val urlLink = response.urlLink
            JsonUtils.readValue<UrlLinkPayload>(urlLink)

        }

    }

    override fun messageSendContact(conversationId: String, contactId: String): Future<String?> {

        val request = Message.MessageSendContactRequest.newBuilder()
            .setContactId(contactId)
            .setConversationId(conversationId)
            .build()


        return CompletableFuture.supplyAsync {
            val response = grpcClient!!.messageSendContact(request)
            response.id.value
        }
    }

    override fun messageSendFile(conversationId: String, file: FileBox): Future<String?> {

        val fileJson = file.toJsonString()

        log.debug("json is {}", fileJson)
        log.debug("json size is {}", fileJson.length)

        val request = Message.MessageSendFileRequest.newBuilder()
            .setConversationId(conversationId)
            .setFilebox(fileJson)
            .build()

        return CompletableFuture.supplyAsync {
            val response = grpcClient!!.messageSendFile(request)
            response.id.value
        }

    }

    override fun messageSendMiniProgram(conversationId: String, miniProgramPayload: MiniProgramPayload): Future<String?> {

        val request = Message.MessageSendMiniProgramRequest.newBuilder()
            .setConversationId(conversationId)
            .setMiniProgram(JsonUtils.write(miniProgramPayload))
            .build()

        return CompletableFuture.supplyAsync {
            val response = grpcClient!!.messageSendMiniProgram(request)
            response.id.value
        }

    }

    override fun messageSendText(conversationId: String, text: String, mentionList: List<String>?): Future<String?> {
        val request = Message.MessageSendTextRequest.newBuilder()
            .setConversationId(conversationId)
            .setText(text)
            .addAllMentonalIds(mentionList)
            .build()

        return CompletableFuture.supplyAsync {
            val response = grpcClient!!.messageSendText(request)
            val stringValue = response.id
            stringValue.value
        }
    }

    override fun messageSendUrl(conversationId: String, urlLinkPayload: UrlLinkPayload): Future<String?> {

        val request = Message.MessageSendUrlRequest.newBuilder()
            .setConversationId(conversationId)
            .setUrlLink(JsonUtils.write(urlLinkPayload))
            .build()

        return CompletableFuture.supplyAsync {
            val response = grpcClient!!.messageSendUrl(request)
            response.id.value
        }

    }


    override fun messageRecall(messageId: String): Future<Boolean> {
        val request = Message.MessageRecallRequest.newBuilder()
            .setId(messageId)
            .build()

        return CompletableFuture.supplyAsync {
            val response = grpcClient!!.messageRecall(request)
            response.success
        }
    }

    override fun messageRawPayload(messageId: String): Future<MessagePayload> {

        val request = Message.MessagePayloadRequest.newBuilder().setId(messageId).build()

        return CompletableFuture.supplyAsync {
            val response = grpcClient!!.messagePayload(request)
            val payload = MessagePayload(messageId)
            payload.filename = response.filename
            payload.fromId = response.fromId
            payload.text = response.text
            payload.mentionIdList = response.mentionIdsList
            payload.roomId = response.roomId
            payload.timestamp = response.timestamp
            payload.type = MessageType.getByCode(response.type.number)
            payload.toId = response.toId
            payload
        }

    }

    override fun messageRawPayloadParser(rawPayload: MessagePayload): Future<MessagePayload> {
        return CompletableFuture.completedFuture(rawPayload)
    }

    override fun roomInvitationAccept(roomInvitation: String): Future<Void> {

        val request = RoomInvitation.RoomInvitationAcceptRequest.newBuilder()
            .setId(roomInvitation)
            .build()

        return CompletableFuture.supplyAsync {
            grpcClient!!.roomInvitationAccept(request)
            null
        }
    }

    override fun roomInvitationRawPayload(roomInvitationId: String): Future<RoomInvitationPayload> {

        val request = RoomInvitation.RoomInvitationPayloadRequest.newBuilder()
            .setId(roomInvitationId)
            .build()

        return CompletableFuture.supplyAsync {
            val response = grpcClient!!.roomInvitationPayload(request)

            val payload = RoomInvitationPayload()

            payload.avatar = response.avatar
            payload.id = response.id
            payload.invitation = response.invitation
            payload.inviterId = response.inviterId
            payload.memberCount = response.memberCount
            payload.memberIdList = response.memberIdsList
            payload.receiverId = response.receiverId
            payload.timestamp = response.timestamp
            payload.topic = response.topic

            payload
        }

    }

    override fun roomInvitationRawPayloadParser(rawPayload: RoomInvitationPayload): Future<RoomInvitationPayload> {
        return CompletableFuture.completedFuture(rawPayload)
    }

    override fun roomAdd(roomId: String, contactId: String): Future<Void> {

        val request = Room.RoomAddRequest.newBuilder()
            .setContactId(contactId)
            .setId(roomId)
            .build()

        return CompletableFuture.supplyAsync {
            grpcClient!!.roomAdd(request)
            return@supplyAsync null
        }

    }

    override fun roomAvatar(roomId: String): Future<FileBox> {

        val request = Room.RoomAvatarRequest.newBuilder()
            .setId(roomId)
            .build()

        return CompletableFuture.supplyAsync {

            val response = grpcClient!!.roomAvatar(request)
            val filebox = response.filebox
            FileBox.fromJson(filebox)

        }


    }

    override fun roomCreate(contactIdList: List<String>, topic: String?): Future<String> {

        val request = Room.RoomCreateRequest.newBuilder()
            .setTopic(topic)
            .addAllContactIds(contactIdList)
            .build()

        return CompletableFuture.supplyAsync {
            val response = grpcClient!!.roomCreate(request)
            response.id
        }


    }

    override fun roomDel(roomId: String, contactId: String): Future<Void> {

        val request = Room.RoomDelRequest.newBuilder()
            .setId(roomId)
            .setContactId(contactId)
            .build()

        return CompletableFuture.supplyAsync {
            grpcClient!!.roomDel(request)
            null
        }

    }

    override fun roomList(): Future<List<String>> {

        val request = Room.RoomListRequest.newBuilder().build()

        return CompletableFuture.supplyAsync {
            val response = grpcClient!!.roomList(request)
            response.idsList
        }

    }

    override fun roomQRCode(roomId: String): Future<String> {

        val request = Room.RoomQRCodeRequest.newBuilder()
            .setId(roomId)
            .build()


        return CompletableFuture.supplyAsync {
            val response = grpcClient!!.roomQRCode(request)
            response.qrcode
        }

    }

    override fun roomQuit(roomId: String): Future<Void> {

        val request = Room.RoomQuitRequest.newBuilder()
            .setId(roomId)
            .build()

        return CompletableFuture.supplyAsync {
            grpcClient!!.roomQuit(request)
            return@supplyAsync null
        }

    }

    override fun roomTopic(roomId: String): Future<String?>? {

        val request = Room.RoomTopicRequest.newBuilder()
            .setId(roomId)
            .build()

        return CompletableFuture.supplyAsync {
            val response = grpcClient!!.roomTopic(request)
            response.topic.value
        }


    }

    override fun roomTopic(roomId: String, topic: String): Future<Void> {

        val value = StringValue.newBuilder().setValue(topic)

        val request = Room.RoomTopicRequest.newBuilder()
            .setId(roomId)
            .setTopic(value)
            .build()

        return CompletableFuture.supplyAsync {
            grpcClient!!.roomTopic(request)
            null
        }
    }

    override fun roomRawPayload(roomId: String): Future<RoomPayload> {

        val request = Room.RoomPayloadRequest.newBuilder()
            .setId(roomId)
            .build()

        return CompletableFuture.supplyAsync {
            val response = grpcClient!!.roomPayload(request)
            val payload = RoomPayload(response.id)

            payload.adminIdList = response.adminIdsList
            payload.avatar = response.avatar
            payload.memberIdList = response.memberIdsList
            payload.ownerId = response.ownerId
            payload.topic = response.topic
            payload
        }

    }

    override fun roomRawPayloadParser(roomPayload: RoomPayload): Future<RoomPayload> {
        return CompletableFuture.completedFuture(roomPayload)
    }

    override fun getRoomAnnounce(roomId: String): Future<String> {

        val request = Room.RoomAnnounceRequest.newBuilder()
            .setId(roomId)
            .build()

        return CompletableFuture.supplyAsync {
            val response = grpcClient!!.roomAnnounce(request)
            response.text.value
        }


    }

    override fun setRoomAnnounce(roomId: String, text: String): Future<Void> {

        val value = StringValue.newBuilder().setValue(text)

        val request = Room.RoomAnnounceRequest.newBuilder()
            .setId(roomId)
            .setText(value)
            .build()

        return CompletableFuture.supplyAsync {
            grpcClient!!.roomAnnounce(request)
            return@supplyAsync null
        }


    }

    override fun roomMemberList(roomId: String): Future<List<String>> {

        val request = RoomMember.RoomMemberListRequest.newBuilder()
            .setId(roomId)
            .build()

        return CompletableFuture.supplyAsync {
            val response = grpcClient!!.roomMemberList(request)
            response.memberIdsList
        }


    }

    override fun roomMemberRawPayload(roomId: String, contactId: String): Future<RoomMemberPayload> {

        val request = RoomMember.RoomMemberPayloadRequest.newBuilder()
            .setId(roomId)
            .setMemberId(contactId)
            .build()

        return CompletableFuture.supplyAsync {
            val response = grpcClient!!.roomMemberPayload(request)
            val payload = RoomMemberPayload()

            payload.avatar = response.avatar
            payload.id = response.id
            payload.inviterId = response.inviterId
            payload.name = response.name
            payload.roomAlias = response.roomAlias
            payload
        }

    }

    override fun roomMemberRawPayloadParser(rawPayload: RoomMemberPayload): Future<RoomMemberPayload> {
        return CompletableFuture.completedFuture(rawPayload)
    }

    private fun onGrpcStreamEvent(event: Event.EventResponse) {

        try {

            val type = event.type
            val payload = event.payload

            log.debug("PuppetHostie $type payload $payload")

            if (type != Event.EventType.EVENT_TYPE_HEARTBEAT) {
                emit(EventEnum.HEART_BEAT, EventHeartbeatPayload("heartbeat",6000))
            }

            when (type) {
                Event.EventType.EVENT_TYPE_DONG -> emit(EventEnum.DONG, JsonUtils.readValue<EventDongPayload>(payload))

                Event.EventType.EVENT_TYPE_ERROR -> {
                    emit(EventEnum.ERROR, JsonUtils.readValue<EventErrorPayload>(payload))
                }

                Event.EventType.EVENT_TYPE_HEARTBEAT -> {
                    val heartbeatPayload = JsonUtils.readValue<EventHeartbeatPayload>(payload)
                    emit(EventEnum.HEART_BEAT, heartbeatPayload)
                }

                Event.EventType.EVENT_TYPE_FRIENDSHIP -> {
                    val friendshipPayload = JsonUtils.readValue<EventFriendshipPayload>(payload)
                    emit(EventEnum.FRIENDSHIP, friendshipPayload)
                }

                Event.EventType.EVENT_TYPE_LOGIN -> {
                    val loginPayload = JsonUtils.readValue<EventLoginPayload>(payload)
                    setId(loginPayload.contactId)
                    emit(EventEnum.LOGIN, loginPayload)
                }

                Event.EventType.EVENT_TYPE_LOGOUT -> {
                    this.setId("")
                    emit(EventEnum.LOGOUT, JsonUtils.readValue<EventLogoutPayload>(payload))
                }


                Event.EventType.EVENT_TYPE_MESSAGE -> {
                    val eventMessagePayload = JsonUtils.readValue<EventMessagePayload>(payload)
                    emit(EventEnum.MESSAGE, eventMessagePayload)
                }

                Event.EventType.EVENT_TYPE_READY -> {
                    emit(EventEnum.READY, JsonUtils.readValue<EventReadyPayload>(payload))
                }

                Event.EventType.EVENT_TYPE_ROOM_INVITE -> {
                    emit(EventEnum.ROOM_INVITE, JsonUtils.readValue<EventRoomInvitePayload>(payload))
                }

                Event.EventType.EVENT_TYPE_ROOM_JOIN -> {
                    emit(EventEnum.ROOM_JOIN, JsonUtils.readValue<EventRoomJoinPayload>(payload))
                }

                Event.EventType.EVENT_TYPE_ROOM_LEAVE -> {
                    emit(EventEnum.ROOM_LEAVE, JsonUtils.readValue<EventRoomLeavePayload>(payload))
                }

                Event.EventType.EVENT_TYPE_ROOM_TOPIC -> {
                    emit(EventEnum.ROOM_TOPIC, JsonUtils.readValue<EventRoomTopicPayload>(payload))
                }

                Event.EventType.EVENT_TYPE_SCAN -> {
                    val eventScanPayload = JsonUtils.readValue<EventScanPayload>(payload)
                    log.debug("scan pay load is {}", eventScanPayload)
                    emit(EventEnum.SCAN, eventScanPayload)
                }

                Event.EventType.EVENT_TYPE_RESET -> {
                    log.warn("got an Event type reset")
                }

                Event.EventType.EVENT_TYPE_UNSPECIFIED -> {
                    log.error("got an Event type unspecified")
                }


                else -> {
                    log.debug("PuppetHostie $type payload $payload")
                }


            }
        } catch (e: Exception) {
            log.error("error", e)
        }

    }


    companion object {
        private val log = LoggerFactory.getLogger(GrpcPuppet::class.java)
    }

}

