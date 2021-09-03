package com.app.mqtt.netty;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;


/**
 * 对MQTT客户端发送消息后，处理的返回消息，基于MQTT协议的，需要MQTT协议的主要内容
 *
 * MQTT拥有14种不同的消息类型MqttMessageType.：
 * CONNECT：客户端连接到MQTT代理
 * CONNACK：连接确认
 * PUBLISH：新发布消息
 * PUBACK：新发布消息确认，是QoS 1给PUBLISH消息的回复
 * PUBREC：QoS 2消息流的第一部分，表示消息发布已记录
 * PUBREL：QoS 2消息流的第二部分，表示消息发布已释放
 * PUBCOMP：QoS 2消息流的第三部分，表示消息发布完成
 * SUBSCRIBE：客户端订阅某个主题
 * SUBACK：对于SUBSCRIBE消息的确认
 * UNSUBSCRIBE：客户端终止订阅的消息
 * UNSUBACK：对于UNSUBSCRIBE消息的确认
 * PINGREQ：心跳
 * PINGRESP：确认心跳
 * DISCONNECT：客户端终止连接前优雅地通知MQTT代理
 *
 * QoS 等级
 * 为适应设备不同的网络环境，MQTT 设计了 3 个 QoS 等级，0, 1, 2:
 *
 * At most once (0)
 * At least once (1)
 * Exactly once (2)
 * QoS 0 是一种 “fire and forget” 的消息发送模式：Sender (可能是 Publisher 或者 Broker) 发送一条消息之后，就不再关心它有没有发送到对方，也不设置任何重发机制。
 *
 * QoS 1 包含了简单的重发机制，Sender 发送消息之后等待接收者的 ACK，如果没收到 ACK 则重新发送消息。这种模式能保证消息至少能到达一次，但无法保证消息重复。
 *
 * QoS 2 设计了略微复杂的重发和重复消息发现机制，保证消息到达对方并且严格只到达一次。
 */
@Slf4j
public class BootMqttMsgBack {

    /**
     * 确认连接请求
     *
     * @param channel
     * @param mqttMessage
     */
    public static void connAck(Channel channel, MqttMessage mqttMessage) {
        MqttConnectMessage mqttConnectMessage = (MqttConnectMessage) mqttMessage;
        MqttFixedHeader mqttFixedHeaderInfo = mqttConnectMessage.fixedHeader();
        MqttConnectVariableHeader mqttConnectVariableHeaderInfo = mqttConnectMessage.variableHeader();

        //	构建返回报文， 可变报头
        MqttConnAckVariableHeader mqttConnAckVariableHeaderBack = new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, mqttConnectVariableHeaderInfo.isCleanSession());
        //	构建返回报文， 固定报头
        MqttFixedHeader mqttFixedHeaderBack = new MqttFixedHeader(MqttMessageType.CONNACK, mqttFixedHeaderInfo.isDup(), MqttQoS.AT_MOST_ONCE, mqttFixedHeaderInfo.isRetain(), 0x02);
        //	构建CONNACK消息体
        MqttConnAckMessage connAck = new MqttConnAckMessage(mqttFixedHeaderBack, mqttConnAckVariableHeaderBack);
        log.warn("connAck :" + connAck.toString());
        channel.writeAndFlush(connAck);
    }

    /**
     * 根据qos发布确认
     *
     * @param channel
     * @param mqttMessage
     */
    public static void publish(Channel channel, MqttMessage mqttMessage) {
        MqttPublishMessage mqttPublishMessage = (MqttPublishMessage) mqttMessage;
        MqttFixedHeader mqttFixedHeaderInfo = mqttPublishMessage.fixedHeader();
        MqttQoS qos = mqttFixedHeaderInfo.qosLevel();
        byte[] headBytes = new byte[mqttPublishMessage.payload().readableBytes()];
        mqttPublishMessage.payload().readBytes(headBytes);
        String data = new String(headBytes);
        log.info("publish qosLevel = {}, data : {}", qos, data);

        switch (qos) {
            case AT_MOST_ONCE:        //	至多一次
                break;
            case AT_LEAST_ONCE:        //	至少一次
                //	构建返回报文， 可变报头
                MqttMessageIdVariableHeader mqttMessageIdVariableHeaderBack = MqttMessageIdVariableHeader.from(mqttPublishMessage.variableHeader().packetId());
                //	构建返回报文， 固定报头
                MqttFixedHeader mqttFixedHeaderBack = new MqttFixedHeader(MqttMessageType.PUBACK, mqttFixedHeaderInfo.isDup(), MqttQoS.AT_MOST_ONCE, mqttFixedHeaderInfo.isRetain(), 0x02);
                //	构建PUBACK消息体
                MqttPubAckMessage pubAck = new MqttPubAckMessage(mqttFixedHeaderBack, mqttMessageIdVariableHeaderBack);
                log.info("pubAck : {}", pubAck.toString());
                channel.writeAndFlush(pubAck);
                break;
            case EXACTLY_ONCE:        //	刚好一次
                //	构建返回报文， 固定报头
                MqttFixedHeader mqttFixedHeaderBack2 = new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_LEAST_ONCE, false, 0x02);
                //	构建返回报文， 可变报头
                MqttMessageIdVariableHeader mqttMessageIdVariableHeaderBack2 = MqttMessageIdVariableHeader.from(mqttPublishMessage.variableHeader().packetId());
                MqttMessage mqttMessageBack = new MqttMessage(mqttFixedHeaderBack2, mqttMessageIdVariableHeaderBack2);
                log.info("mqttMessageBack : {}", mqttMessageBack.toString());
                channel.writeAndFlush(mqttMessageBack);
                break;
            default:
                break;
        }
    }

    /**
     * 发布完成 qos2
     *
     * @param channel
     * @param mqttMessage
     */
    public static void pubComp(Channel channel, MqttMessage mqttMessage) {
        MqttMessageIdVariableHeader messageIdVariableHeader = (MqttMessageIdVariableHeader) mqttMessage.variableHeader();
        //	构建返回报文， 固定报头
        MqttFixedHeader mqttFixedHeaderBack = new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 0x02);
        //	构建返回报文， 可变报头
        MqttMessageIdVariableHeader mqttMessageIdVariableHeaderBack = MqttMessageIdVariableHeader.from(messageIdVariableHeader.messageId());
        MqttMessage mqttMessageBack = new MqttMessage(mqttFixedHeaderBack, mqttMessageIdVariableHeaderBack);
        log.info("pubComp : {}", mqttMessageBack.toString());
        channel.writeAndFlush(mqttMessageBack);
    }

    /**
     * 订阅确认
     *
     * @param channel
     * @param mqttMessage
     */
    public static void subAsk(Channel channel, MqttMessage mqttMessage) {
        MqttSubscribeMessage mqttSubscribeMessage = (MqttSubscribeMessage) mqttMessage;
        MqttMessageIdVariableHeader messageIdVariableHeader = mqttSubscribeMessage.variableHeader();
        //	构建返回报文， 可变报头
        MqttMessageIdVariableHeader variableHeaderBack = MqttMessageIdVariableHeader.from(messageIdVariableHeader.messageId());
        Set<String> topics = mqttSubscribeMessage.payload().topicSubscriptions().stream().map(mqttTopicSubscription -> mqttTopicSubscription.topicName()).collect(Collectors.toSet());
        //log.info(topics.toString());
        List<Integer> grantedQoSLevels = new ArrayList<>(topics.size());
        for (int i = 0; i < topics.size(); i++) {
            grantedQoSLevels.add(mqttSubscribeMessage.payload().topicSubscriptions().get(i).qualityOfService().value());
        }
        //	构建返回报文	有效负载
        MqttSubAckPayload payloadBack = new MqttSubAckPayload(grantedQoSLevels);
        //	构建返回报文	固定报头
        MqttFixedHeader mqttFixedHeaderBack = new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 2 + topics.size());
        //	构建返回报文	订阅确认
        MqttSubAckMessage subAck = new MqttSubAckMessage(mqttFixedHeaderBack, variableHeaderBack, payloadBack);
        log.info("suback : {}", subAck.toString());
        channel.writeAndFlush(subAck);
    }

    /**
     * 取消订阅确认
     *
     * @param channel
     * @param mqttMessage
     */
    public static void unSubAck(Channel channel, MqttMessage mqttMessage) {
        MqttMessageIdVariableHeader messageIdVariableHeader = (MqttMessageIdVariableHeader) mqttMessage.variableHeader();
        //	构建返回报文	可变报头
        MqttMessageIdVariableHeader variableHeaderBack = MqttMessageIdVariableHeader.from(messageIdVariableHeader.messageId());
        //	构建返回报文	固定报头
        MqttFixedHeader mqttFixedHeaderBack = new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 2);
        //	构建返回报文	取消订阅确认
        MqttUnsubAckMessage unSubAck = new MqttUnsubAckMessage(mqttFixedHeaderBack, variableHeaderBack);
        log.info("unsuback : {}", unSubAck.toString());
        channel.writeAndFlush(unSubAck);
    }

    /**
     * 心跳响应
     *
     * @param channel
     * @param mqttMessage
     */
    public static void pingResp(Channel channel, MqttMessage mqttMessage) {
        //	心跳响应报文	11010000 00000000  固定报文
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessage mqttMessageBack = new MqttMessage(fixedHeader);
        log.info("pingresp : {} ", mqttMessageBack.toString());
        channel.writeAndFlush(mqttMessageBack);
    }


}
