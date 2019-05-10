package com.aliyun.openservice.iot.as.bridge.demo.core;

import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.aliyun.iot.as.bridge.core.BridgeBootstrap;
import com.aliyun.iot.as.bridge.core.config.ConfigFactory;
import com.aliyun.iot.as.bridge.core.handler.DownlinkChannelHandler;
import com.aliyun.iot.as.bridge.core.handler.tsl.TslUplinkHandler;
import com.aliyun.iot.as.bridge.core.model.DeviceIdentity;
import com.aliyun.iot.as.bridge.core.model.ProtocolMessage;
import com.aliyun.iot.as.bridge.core.model.Session;
import com.aliyun.iot.as.bridge.core.model.tsl.ThingEventTypes;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BridgeTslDemo {

    private static Logger log = LoggerFactory.getLogger(BridgeTslDemo.class);

    private static BridgeBootstrap bridgeBootstrap;

    /**
     * self-define topic template created in IoT Platform Web Console
     */
    private final static String TOPIC_TEMPLATE_USER_DEFINE = "/%s/%s/user/update";

    private final static String PROP_POST_PAYLOAD_TEMPLATE = "Hello IoT Bridge";

private static ExecutorService executorService  = new ThreadPoolExecutor(
    Runtime.getRuntime().availableProcessors(),
    Runtime.getRuntime().availableProcessors() * 2,
    60, TimeUnit.SECONDS,
    new LinkedBlockingQueue<>(1000),
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("bridge-downlink-handle-%d").build(),
    new ThreadPoolExecutor.AbortPolicy());

    private final static Random random = new Random();

    public static void main(String args[]) {
        //Use application.conf & devices.conf by default
        bridgeBootstrap = new BridgeBootstrap();
        bridgeBootstrap.bootstrap(new DownlinkChannelHandler() {
            @Override
            public boolean pushToDevice(Session session, String topic, byte[] payload) {
                //get message from cloud
                //get downlink message from cloud
                executorService.submit(() -> handleDownLinkMessage(session, topic, payload));
                return true;
            }

            @Override
            public boolean broadcast(String s, byte[] bytes) {
                return false;
            }
        });
        log.info("======== Bridge bootstrap success =========");
        //original device identity, defined in devices.conf
        String originalIdentity = "demoDevice1";

        //device Session of your specific protocol, If none, you can just use empty object
        Object originalChannel = new Object();
        Session session = Session.newInstance(originalIdentity, originalChannel);
        //device online
        TslUplinkHandler tslUplinkHandler = new TslUplinkHandler();
        tslUplinkHandler.doOnline(session, originalIdentity);

        //report property
        //Property 'testProp' is defined in IoT Platform Web Console
        String requestId = String.valueOf(random.nextInt(1000));
        tslUplinkHandler.reportProperty(requestId, originalIdentity, "testProp", random.nextInt(100));

        //fire event
        //Event 'testEvent' is defined in IoT Platform Web Console
        requestId = String.valueOf(random.nextInt(1000));
        HashMap<String, Object> params = new HashMap<String, Object>();
        params.put("testEventParam", 123);
        tslUplinkHandler.fireEvent(originalIdentity, "testEvent", ThingEventTypes.INFO, params);

        //update device tag
        //Tag 'testDeviceTag' is defined in IoT Platform Web Console
        requestId = String.valueOf(random.nextInt(1000));
        tslUplinkHandler.updateDeviceTag(requestId, originalIdentity, "testDeviceTag", String.valueOf(random.nextInt(1000)));

        //pub self-define topic
        //Topic Template ${TOPIC_TEMPLATE_USER_DEFINE} is defined in IoT Platform Web Console
        DeviceIdentity deviceIdentity = ConfigFactory.getDeviceConfigManager().getDeviceIdentity(originalIdentity);
        ProtocolMessage protocolMessage = new ProtocolMessage();
        protocolMessage.setPayload(PROP_POST_PAYLOAD_TEMPLATE.getBytes());
        protocolMessage.setQos(0);
        protocolMessage.setTopic(String.format(TOPIC_TEMPLATE_USER_DEFINE, deviceIdentity.getProductKey(), deviceIdentity.getDeviceName()));
        tslUplinkHandler.doPublishAsync(originalIdentity, protocolMessage);
    }

    private static void handleDownLinkMessage(Session session, String topic, byte[] payload) {
        String content = new String(payload);
        log.info("Get DownLink message, session:{}, topic:{}, content:{}", session, topic, content);
        Object channel = session.getChannel();
        String originalIdentity = session.getOriginalIdentity();
        //for example, you can send the message to device via channel, it depends on you specific server implementation
    }
}
