import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class test {
    private static String serviceURI= "3310.ddns.net:1883";
    private static String clientID = "3310-u1234567";

    private static MqttClientPersistence persistence = new MemoryPersistence();

    private static String topic = "";

    private static int QoS = 0;

    public static void publish() {
        try{
            MqttClient client = new MqttClient(serviceURI, clientID, persistence);
            MqttConnectOptions connectOptions = new MqttConnectOptions();
            connectOptions.setCleanSession(false);
            client.connect(connectOptions);
            System.out.println("Publisher connect status: " + client.isConnected());

            MqttTopic mqttTopic = client.getTopic(topic);

            MqttMessage mqttMessage = new MqttMessage();
            mqttMessage.setQos(QoS);

            int i = 1;
            String message = "hello";

            while (true){
                String str = message + i++;
                mqttMessage.setPayload(str.getBytes());

                MqttDeliveryToken deliveryToken = mqttTopic.publish(mqttMessage);
                if (!deliveryToken.isComplete()){
                    System.out.println("Publisher announced: " + str + " failed");
                    deliveryToken.waitForCompletion();
                }else {
                    System.out.println("Publisher announced: " + str + " succeeded");
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void subscribe(){
        try{
            MqttClient client = new MqttClient(serviceURI,clientID,persistence);
            client.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable throwable) {
                    System.out.println("Subscriber disconnected...");
                    System.out.println(throwable.getCause());
                }

                @Override
                public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
                    System.out.println("Subscriber receive message: " + mqttMessage.toString());
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

                }
            });
            MqttConnectOptions connectOptions = new MqttConnectOptions();
            connectOptions.setCleanSession(false);
            client.connect(connectOptions);
            client.subscribe(topic,QoS);
            System.out.println("Subscriber status: " + client.isConnected());
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
