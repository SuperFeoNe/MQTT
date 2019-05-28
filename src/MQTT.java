import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import java.util.*;


/**
    1. The rate of messages you receive (message/second)
    2. The rate of message loss you see (percentage)
    3. The rate of duplicated messages you see (percentage)
    4. The rate of out-of-order messages you see (percentage)
 **/


public class MQTT implements MqttCallback {

    private static String acc = "students"; //user name
    private static String pwd = "33106331"; // password
    private static String broker = "tcp://comp3310.ddns.net:1883"; // url
    private static String clientID = "3310-u6089193";
    private static int qos = 2; // Quality of Service level
    private static String topic = "counter/fast/q2";

    private static String message = null;
    static ArrayList<Integer> allMessage = new ArrayList<>(); // all received messages
    static HashSet<Integer> Dup = new HashSet<>(); // all received messages except Duplicated messages
    static ArrayList<Integer> minMessage = new ArrayList<>(); // all received me
    static HashMap<Integer, ArrayList<Integer>> eachMinMessage = new HashMap<>(); // key -> minute , value -> message received in that minute
    //static HashMap<Integer, HashSet<Integer>> eachMinMessageDup = new HashMap<>(); // key -> minute, value -> duplicated
    private static int index = 0;

    @Override
    public void connectionLost(Throwable throwable) {
        System.out.println("Connection Lost. Please try again later.");
    }

    @Override
    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
        //int
        message = new String(mqttMessage.getPayload());
        if (message.length()!=0 && message.charAt(0) >= 48 && message.charAt(0)<= 57) { // check message type
            int addIn = Integer.parseInt(message);
            //System.out.println("This is addIn: " + addIn);
            allMessage.add(addIn);
            Dup.add(addIn);
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        System.out.println("Delivery complete");
        System.out.println(iMqttDeliveryToken.isComplete());
    }

    public void connect() throws MqttException {
        MemoryPersistence persistence = new MemoryPersistence();
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        options.setUserName(acc);
        options.setPassword(pwd.toCharArray());
        options.setConnectionTimeout(20);
        options.setKeepAliveInterval(30);
        options.setAutomaticReconnect(true);
        MqttClient mqttClient = new MqttClient(broker, clientID, persistence);
        mqttClient.setCallback(this);
        mqttClient.connect(options);

        System.out.println("Successful connect to " + broker);

        mqttClient.subscribe(topic, qos);


        for (int i = 0; i < 10; i++) { // 10min
            try{
                Thread.sleep(60000);
            }catch (Exception e){
                e.printStackTrace();
            }
            System.out.println("\n" + "This is " + (i+1) + " times" + "\n");

            if (i == 0){
                minMessage.add(0, allMessage.size()); // add each minute received information numbers
                eachMinMessage.put(0, allMessage);
                System.out.println("This is first time received messages numbers:" + minMessage);
                System.out.println("This is first time all messages: " + allMessage);
                System.out.println( "\n" +"********************************************************************************************************************************************");

            }else {
                index += minMessage.get(i-1); // the index each new added messages position
                int s = allMessage.size();
                minMessage.add(i, allMessage.subList(index, s).size());
                List<Integer> list = allMessage.subList(index, s);
                ArrayList<Integer> addIn = new ArrayList<>();
                addIn.addAll(list);
                ArrayList<Integer> temp = addIn;
                eachMinMessage.put(i, temp);
                //minMessage.add(i, allMessage.size()-minMessage.get(i-1));
                System.out.println("This is " + (i+1) +" times received all messages:" + temp);
                System.out.println("This is " + (i+1) + " times receive messages numbers:" + minMessage);
                System.out.println( "\n" +"********************************************************************************************************************************************");
            }
            //System.out.println("Message received in " + (i+1) + " minutes " + minMessage.get(i));
        }
        //analysis data before mqtt disconnect
        System.out.println(receiveRate());
        System.out.println(msgLoss());
        System.out.println(duplicated());
        System.out.println(outOfOrder());
        try {
            mqttClient.disconnect();
            System.exit(0);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    /**
     * Calculating the message receive rate.
     * The total testing length is 10 minutes
     * Also, calculate the median and mean number to improve statistical accuracy
     *
     * @return mean; median; mode
     */
    public static ArrayList<Double> receiveRate(){
        double allMean = 0;
        allMean = (double)allMessage.size()/(double)600; // the total receive rate in 10 minutes

        ArrayList<Double> temp = new ArrayList<>();
        for (int x: minMessage
        ) {
            temp.add((double)x/60);
        }
        List<Double> list = temp;
        double median = getMedian(list);
        double mode = getMode(temp);
        ArrayList<Double> arr = new ArrayList<>();
        arr.add(allMean);
        arr.add(median);
        arr.add(mode);
        return arr;
    }


    /**
     * Calculating the messages lost
     * Return as percentage
     * Also calculate median and mean value
     *
     * @return
     */
    public static ArrayList<Double> msgLoss(){
        double lossNo = 0; // all loss messages number
        for (int i = 1; i < allMessage.size(); i++) {
            if (allMessage.get(i) - allMessage.get(i-1) != 1) lossNo += 1.0;
        }
        double mean = lossNo / (double)allMessage.size(); // this is total messages lost rate

        //this part get the each minutes lost rate
        ArrayList<Double> minLoss = new ArrayList<>(); //message lost in each minutes
        for (int i = 0; i < eachMinMessage.size(); i++) {
            double eachMinLossNo = 0;
            ArrayList<Integer> temp = eachMinMessage.get(i);
            for (int j = 1; j < temp.size(); j++) {
               if (temp.get(j) - temp.get(j-1) != 1){
                   eachMinLossNo += 1;
               }
            }
            double size = temp.size();
            double addIn = eachMinLossNo/size;
            minLoss.add(addIn);
        }
        List<Double> data = minLoss;
        double median = getMedian(data);
        double mode = getMode(data);
        ArrayList<Double> arr = new ArrayList<>();
        arr.add(mean);
        arr.add(median);
        arr.add(mode);
        return arr;
    }


    /**
     * Calculating the duplicated message.
     * Return as percentage
     * @return
     */
    public static ArrayList<Double> duplicated(){
        double allMsg = allMessage.size();
        double uniqueMsg = Dup.size();
        double mean = (allMsg-uniqueMsg)/ allMsg;

        ArrayList<Double> minDup = new ArrayList<>();// messages lost in each minutes
        for (int i = 0; i < eachMinMessage.size(); i++) {
            HashSet<Integer> hst = new HashSet<>();
            ArrayList<Integer> arr = eachMinMessage.get(i);
            for (int j = 0; j < arr.size(); j++) {
                hst.add(arr.get(j));
            }
            double allSize = arr.size();
            double uniqueSize = hst.size();
            double eachMinLoss = allSize - uniqueSize;
            minDup.add(eachMinLoss);
        }
        List<Double> data = minDup;
        double median = getMedian(data);
        double mode = getMode(data);
        ArrayList<Double> arr = new ArrayList<>();
        arr.add(mean);
        arr.add(median);
        arr.add(mode);
        return arr;
    }


    /**
     * Calculating out-of-order messages.
     * Return as percentage
     * Considering different order:
     *  1. 10, 11, 12, 13, 14, 15
     *  2. 15, 14, 13, 12, 11, 10
     * @return
     */
    public static ArrayList<Double> outOfOrder(){
        int oooNo = 0;
        for (int i = 1; i < allMessage.size(); i++) {
            int pos1 = allMessage.get(i);
            if (pos1 <= allMessage.get(i-1)) oooNo +=1;
        }

        double mean = oooNo/allMessage.size();

        ArrayList<Double> arr = new ArrayList<>(); //out of order messages number in each minutes

        for (int i = 0; i < eachMinMessage.size(); i++) {
            ArrayList<Integer> data = eachMinMessage.get(i);
            Double addIn = 0.0;
            for (int j = 1; j < data.size(); j++) {
                int pos1 = data.get(j);
                if (pos1 <= data.get(j-1)) addIn += 1;
            }
            addIn = addIn/data.size();
            arr.add(addIn);
        }
        List<Double> list = arr;

        double median = getMedian(list);
        double mode = getMode(list);

        ArrayList<Double> rtn = new ArrayList<>();
        rtn.add(mean);
        rtn.add(median);
        rtn.add(mode);
        return rtn;
    }


    /**
     *
     * @param list
     * @return the median number of the list
     */
    public static double getMedian(List<Double> list) {
        Collections.sort(list);
        double median = 0;
        int size = list.size();
        if (size%2 == 0) median = (list.get(size/2) + list.get(size/2 + 1))/2; else median = list.get(size/2); // get median number
        return median;
    }

    public static double getMode(List<Double> list){
        HashMap<Double, Integer> data = new HashMap<>();
        for (int i = 0; i < list.size(); i++) {
            if (data.get(list.get(i)) == null) data.put(list.get(i), 1); else data.put(list.get(i), data.get(list.get(i)) + 1);
        }
        double times = 0;
        double mode = 0;
        for (Double i: data.keySet()
        ) {
            if (data.get(i)> times){
                times = data.get(i);
                mode = i;
            }
        }
        if (times == 0.0) return 0.0; else return mode;
    }


    public static void main(String[] args) throws MqttException{
        System.out.println("Broker is: " + broker);
        System.out.println("Topic is: " + topic);
        System.out.println("QoS: " + qos);
        MQTT mqtt = new MQTT();
        mqtt.connect();
    }
}
