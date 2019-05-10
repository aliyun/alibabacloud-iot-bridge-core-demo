package com.aliyun.openservice.iot.as.bridge.demo.core;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.aliyun.iot.as.bridge.core.BridgeBootstrap;
import com.aliyun.iot.as.bridge.core.config.ConfigFactory;
import com.aliyun.iot.as.bridge.core.handler.DownlinkChannelHandler;
import com.aliyun.iot.as.bridge.core.handler.UplinkChannelHandler;
import com.aliyun.iot.as.bridge.core.model.DeviceIdentity;
import com.aliyun.iot.as.bridge.core.model.ProtocolMessage;
import com.aliyun.iot.as.bridge.core.model.Session;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bridge Gateway Demo for IoT Link Platform Senior Version
 */
public class BridgeBasicDemo {
	
	private static Logger log = LoggerFactory.getLogger(BridgeBasicDemo.class);

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

	public static void main(String[] args) throws Exception {
		//Use application.conf & devices.conf by default
        bridgeBootstrap = new BridgeBootstrap();
        bridgeBootstrap.bootstrap(new DownlinkChannelHandler() {
            @Override
            public boolean pushToDevice(Session session, String topic, byte[] payload) {
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
        UplinkChannelHandler uplinkChannelHandler = new UplinkChannelHandler();
        uplinkChannelHandler.doOnline(session, originalIdentity);

        //pub self-define topic
        //Topic Template ${TOPIC_TEMPLATE_USER_DEFINE} is defined in IoT Platform Web Console
        DeviceIdentity deviceIdentity = ConfigFactory.getDeviceConfigManager().getDeviceIdentity(originalIdentity);
        ProtocolMessage protocolMessage = new ProtocolMessage();
        protocolMessage.setPayload(PROP_POST_PAYLOAD_TEMPLATE.getBytes());
        protocolMessage.setQos(0);
        protocolMessage.setTopic(String.format(TOPIC_TEMPLATE_USER_DEFINE, deviceIdentity.getProductKey(), deviceIdentity.getDeviceName()));
        uplinkChannelHandler.doPublishAsync(originalIdentity, protocolMessage);

	}

	private static void handleDownLinkMessage(Session session, String topic, byte[] payload) {
        String content = new String(payload);
        log.info("Get DownLink message, session:{}, topic:{}, content:{}", session, topic, content);
        Object channel = session.getChannel();
        String originalIdentity = session.getOriginalIdentity();
        //for example, you can send the message to device via channel, it depends on you specific server implementation
    }

}
