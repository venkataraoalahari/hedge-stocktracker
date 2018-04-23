package DataGeneratorPckg;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

class RunTaskHash extends TimerTask {

	int counter = 0;
	String[] StockList;
	int firstExecution = 0;
	Properties prop = new Properties();
	HashMap<String, String> output = new HashMap<String, String>();
	public final SimpleDateFormat tmstmpVal = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public final SimpleDateFormat dtVal = new SimpleDateFormat("yyyy-MM-dd");
	public final String startTime = " 08:59:59";
	public final String endTime = " 16:00:01";
	public final String stringsp = "price";
	public final String stringsv = "volume";
	public final String stringst = "timestampValue";
	public final String stringsn = "symbol";
	public final String propStockDetails = "STOCK_DETAILS";
	ProducerRecord<String, String> producerRecord;
	DecimalFormat twodeci = new DecimalFormat("0.00");
	
	
	Properties kafkaProp = new Properties();
	Producer<String, String> producer;
	String StockTimestamp;
	Date currStrtDate;
	Date currEndDate;
	Random rand = new Random();

	public void loadProp() {
		try {
			prop.load(TestDataGeneratorHash.class.getClassLoader().getResourceAsStream("app.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void setCurrentDate() {

		try {
			currStrtDate = tmstmpVal.parse(dtVal.format(new Date()) + startTime);
			currEndDate = tmstmpVal.parse(dtVal.format(new Date()) + endTime);
			System.out.println("Start time is = " + currStrtDate + "\t End time is = " + currEndDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	public String getProp(String key) {
		return prop.getProperty(key);
	}

	public void getStocks() {
		StockList = getProp(propStockDetails).split(",");
	}

	public void putKafka() {
		System.out.println("Loading data into kafka topic....");
		Iterator<String> it = output.keySet().iterator();
		while (it.hasNext()) {
			String key = (String) it.next();
			String value = output.get(key);
			System.out.println("kafka key = " + key + " \t kafka value = " + value);
			producerRecord = new ProducerRecord<String, String>(getProp("TOPIC"), key, value);
			producer.send(producerRecord);
		}
		System.out.println("Loading data into kafka is finished.");
	}

	public void initializeKafka() {
		kafkaProp.setProperty("bootstrap.servers", getProp("BOOTSTRAP_SERVERS"));
		kafkaProp.setProperty("key.serializer", StringSerializer.class.getName());
		kafkaProp.setProperty("value.serializer", StringSerializer.class.getName());
		// producer acks
		kafkaProp.setProperty("acks", getProp("ACKS"));
		kafkaProp.setProperty("retries", getProp("RETRIES"));
		kafkaProp.setProperty("linger.ms", getProp("LINGER_MS"));

		producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(kafkaProp);

	}

	public void generateData() {
		float randomVal;
		String key, basePrice;
		double newStockPrice = 0, oldStockPrice = 0;
		int volume, randomVolume, newStockVolume, oldStockVolume = 0;
		StringBuilder sb = new StringBuilder();
		String finalOutput;
		System.out.println("Total no. of stocks are : " + StockList.length);

		for (int i = 0; i < StockList.length; i++) {

			sb.delete(0, sb.length());
			key = StockList[i].split(":")[0];

			basePrice = StockList[i].split(":")[1];
			System.out.println("Base Price for stock " + key + " is = " + basePrice);

			randomVal = (rand.nextFloat() * (rand.nextBoolean() ? -1 : 1))/100;
			randomVolume = rand.nextInt(3) + 1;
			System.out.println("Random float value is = " + randomVal);
			System.out.println("Random Int volumn is = " + randomVolume);

			if (output.containsKey(key)) {
				oldStockPrice = Double.parseDouble(output.get(key).split(",")[2].split(":")[1].trim());
				oldStockVolume = Integer.parseInt(output.get(key).split(",")[3].split(":")[1].replace("}", "").trim());
				newStockPrice = oldStockPrice + (oldStockPrice * randomVal);
				newStockVolume = oldStockVolume + randomVolume;
				System.out.println("Old Stock price is = " + oldStockPrice);
				System.out.println("Old Stock volume is = " + oldStockVolume);
				System.out.println("New Stock price is = " + newStockPrice);
				System.out.println("New Stock volume is = " + newStockVolume);
			} else {
				System.out.println("Executing generateData() first time...");
				newStockPrice = Double.parseDouble(basePrice);
				newStockVolume = 0;
				System.out.println("Stock price is = " + newStockPrice);
				System.out.println("Stock volume is = " + newStockVolume);

			}

			sb.append("{\"" + stringsn + "\":\"" + key + "\" , ")
					.append("\"" + stringst + "\" : \"" + StockTimestamp + "\" , ")
					.append("\"" + stringsp + "\" : " + Double.toString(newStockPrice) + " , ")
					.append("\"" + stringsv + "\" : " + Integer.toString(newStockVolume) + " } ");

			System.out.println("Key = " + key);
			System.out.println("Value = " + sb.toString());

			output.put(key, sb.toString());

		}
		// putKafka();
	}

	@Override
	public void run() {
		counter++;
		
		if(counter >5)
			System.exit(-1);
		
		System.out.println("********** Inside Run **********");
		
		if (firstExecution == 0) {
			System.out.println("First time executing code...");
			System.out.println("Initialzing values...");
			System.out.println("Loading properties...");
			loadProp();
			System.out.println("Initialzing dates...");
			setCurrentDate();
			System.out.println("Initialzing stock symbols...");
			getStocks();
			System.out.println("Initialzing kafka producer...");
			initializeKafka();
			firstExecution = 1;
		}
		
		StockTimestamp = tmstmpVal.format(new Date());

		try {
			StockTimestamp = tmstmpVal.format(new Date());
			Date currTime = tmstmpVal.parse(StockTimestamp);
			if ((currTime.after(currStrtDate)) && (currTime.before(currEndDate))) {
				System.out.println("Current Time is inbetween given time range.");
				System.out.println("calling generateData....");
				generateData();
			} else {
				System.out.println("Current TIme is outside given time range.");
				System.exit(-1);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		//generateData();
		//putKafka();
	}

}

public class TestDataGeneratorHash {

	public static void main(String[] args) {
		System.out.println("Inside main method of public class.");
		TimerTask task = new RunTaskHash();
		Timer timer = new Timer();
		timer.schedule(task, 0, 1000);
	}
}
