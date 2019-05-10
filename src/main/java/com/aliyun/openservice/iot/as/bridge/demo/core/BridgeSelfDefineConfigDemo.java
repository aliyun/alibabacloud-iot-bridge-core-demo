package com.aliyun.openservice.iot.as.bridge.demo.core;

import java.util.Map;

import com.aliyun.iot.as.bridge.core.BridgeBootstrap;
import com.aliyun.iot.as.bridge.core.config.ConfigFactory;
import com.aliyun.iot.as.bridge.core.config.DeviceConfigManager;
import com.aliyun.iot.as.bridge.core.handler.DownlinkChannelHandler;
import com.aliyun.iot.as.bridge.core.model.DeviceIdentity;
import com.aliyun.iot.as.bridge.core.model.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BridgeSelfDefineConfigDemo {

    private static Logger log = LoggerFactory.getLogger(BridgeSelfDefineConfigDemo.class);

    private static BridgeBootstrap bridgeBootstrap;

    private static Map<String, DeviceIdentity> devicesMap;

    public static void main(String args[]) {
        //self define config
        //you can specify config file location path
        //or you can create an instance and implement the corresponding interface
        //Config.init() must be called before bridgeBootstrap.bootstrap()
        ConfigFactory.init(
            ConfigFactory.getBridgeConfigManager("application-self-define.conf"),
            selfDefineDeviceConfigManager);

        bridgeBootstrap.bootstrap(new DownlinkChannelHandler() {
            @Override
            public boolean pushToDevice(Session session, String topic, byte[] payload) {
                //get downlink message from cloud, do not block here
                String content = new String(payload);
                log.info("Get DownLink message, session:{}, topic:{}, content:{}", session, topic, content);
                return true;
            }

            @Override
            public boolean broadcast(String s, byte[] bytes) {
                return false;
            }
        });
        log.info("======== Bridge bootstrap success =========");
    }

    private static DeviceConfigManager selfDefineDeviceConfigManager = new DeviceConfigManager() {
        @Override
        public DeviceIdentity getDeviceIdentity(String originalIdentity) {
            //imagine you dynamically get deviceInfo in other ways
            return devicesMap.get(originalIdentity);
        }

        @Override
        public String getOriginalIdentity(String productKey, String deviceName) {
            //you can ignore this
            return null;
        }
    };

}
