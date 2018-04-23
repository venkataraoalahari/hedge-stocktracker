package DataGeneratorPckg;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class DataForGivenTimeFrame {

	int counter = 0;
	String[] StockList;
	int firstExecution = 0;
	Properties p = new Properties();
	CustomProperties stockProp = new CustomProperties();
	LinkedHashMap<String, String> output = new LinkedHashMap<String, String>();
	public final SimpleDateFormat tmstmpVal = new SimpleDateFormat("yyyy-MM-dd HH|mm|ss");
	public final SimpleDateFormat dtVal = new SimpleDateFormat("yyyy-MM-dd");
	public final String stringsp = "price";
	public final String stringsv = "volume";
	public final String stringst = "timestampValue";
	public final String stringsn = "symbol";
	public final String propStockDetails = "STOCK_DETAILS";
	public final String propFilePath = "app.properties";
	public final String stockPropFilePath = "FILE_PATH";
	ProducerRecord<String, String> producerRecord;
	DecimalFormat twodeci = new DecimalFormat("0.00");
	int wrongDataPos = 0;
	int wrongDataFlag = 0;
	int wrongDataCounter = 0;
	String oldkeyforWrongData = "";
	double oldPriceBeforeWrongData = 0;
	int oldVolumeBeforeWrongData = 0 ;

	Properties kafkaProp = new Properties();
	Producer<String, String> producer;
	Timestamp StockTimestamp;
	Timestamp currStrtDate;
	Timestamp currEndDate;
	Random rand = new Random();
	Calendar cal = Calendar.getInstance();

	File file;

	public void loadProp() {
		try {
			p.load(DataForGivenTimeFrame.class.getClassLoader().getResourceAsStream(propFilePath));
			file = new File(p.getProperty(stockPropFilePath));
			FileInputStream fileInput = new FileInputStream(file);
			stockProp.load(fileInput);
			fileInput.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void setCurrentDate(String strtDt, String endDt) {
		//System.out.println("First argument is = " + strtDt + "\t Second argument is = " + endDt);
		currStrtDate = Timestamp.valueOf(strtDt);
		currEndDate = Timestamp.valueOf(endDt);
		//System.out.println("Start time is = " + currStrtDate + "\t End time is = " + currEndDate);
	}

	public String getProp(String key) {
		return stockProp.getProperty(key);
	}

	public void getStocks() {
		StockList = getProp(propStockDetails).split(",");
	}

	public void putKafka() {
		//System.out.println("Loading data into kafka topic....");
		Iterator<String> it = output.keySet().iterator();
		while (it.hasNext()) {
			String key = (String) it.next();
			String value = output.get(key);
			//System.out.println("kafka key = " + key + " \t kafka value = " + value);
			producerRecord = new ProducerRecord<String, String>(getProp("TOPIC"), key, value);
			producer.send(producerRecord);
		}
		//System.out.println("Loading data into kafka is finished.");
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
		float randomVal = 0;
		String key, basePrice;
		double newStockPrice = 0, oldStockPrice = 0 ;
		int volume = 0, randomVolume = 0, newStockVolume = 0, oldStockVolume = 0 ;
		StringBuilder sb = new StringBuilder();
		String finalOutput;
		//System.out.println("Total no. of stocks are | " + StockList.length);

		for (int i = 0; i < StockList.length; i++) {

			//System.out.println(StockList[i]);
			sb.delete(0, sb.length());
			key = StockList[i].split(":")[0];
			//System.out.println(key);
			basePrice = StockList[i].split(":")[1];
			//System.out.println("Base Price for stock " + key + " is = " +basePrice);

			if (wrongDataFlag == 1) {
				wrongDataCounter++;
				if (wrongDataCounter == wrongDataPos) {
					//System.out.println("Inside wrong data generation part...");
					randomVal = (rand.nextFloat() * (rand.nextBoolean() ? -1 : 1)) / 10;
					randomVolume = rand.nextInt(3) + 1;
					//System.out.println("Random float value is = " + randomVal);
					//System.out.println("Random Int volumn is = " + randomVolume);
				}else {
					//System.out.println("Inside correct data generation part...");
					randomVal = (rand.nextFloat() * (rand.nextBoolean() ? -1 : 1)) / 100;
					randomVolume = rand.nextInt(3) + 1;
					//System.out.println("Random float value is = " + randomVal);
					//System.out.println("Random Int volumn is = " + randomVolume);
				}	
			}

			if (output.containsKey(key)) {
				//System.out.println("Stock already exists...");
				if (wrongDataFlag == 1 && wrongDataCounter == wrongDataPos) {
					oldkeyforWrongData = key;
					System.out.println(key);
					System.out.println("Generating wrong data for key = " + oldkeyforWrongData);
					oldPriceBeforeWrongData = Double.parseDouble(output.get(key).split(",")[2].split(":")[1].trim());
					oldVolumeBeforeWrongData = Integer.parseInt(output.get(key).split(",")[3].split(":")[1].replace("}", "").trim());
					newStockPrice = oldPriceBeforeWrongData + (oldPriceBeforeWrongData * randomVal);
					newStockVolume = oldVolumeBeforeWrongData + randomVolume;
					//System.out.println("Old Stock price is = " + oldPriceBeforeWrongData);
					//System.out.println("Old Stock volume is = " + oldVolumeBeforeWrongData);
					//System.out.println("New Stock price is = " + newStockPrice);
					//System.out.println("New Stock volume is = " + newStockVolume);
					setWrongDataPos();
				} else if (wrongDataFlag == 1 && oldkeyforWrongData.equals(key)) {
					System.out.println("Generating correct data after wrong data is generated.");
					oldStockPrice = oldPriceBeforeWrongData;
					oldStockVolume = oldVolumeBeforeWrongData;
					newStockPrice = oldStockPrice + (oldStockPrice * randomVal);
					newStockVolume = oldStockVolume + randomVolume;
					oldkeyforWrongData = "";
					//System.out.println("Old Stock price is = " + oldStockPrice);
					//System.out.println("Old Stock volume is = " + oldStockVolume);
					//System.out.println("New Stock price is = " + newStockPrice);
					//System.out.println("New Stock volume is = " + newStockVolume);

				} else {
					//System.out.println("Generating correct data.");
					oldStockPrice = Double.parseDouble(output.get(key).split(",")[2].split(":")[1].trim());
					oldStockVolume = Integer.parseInt(output.get(key).split(",")[3].split(":")[1].replace("}", "").trim());
					newStockPrice = oldStockPrice + (oldStockPrice * randomVal);
					newStockVolume = oldStockVolume + randomVolume;
					//System.out.println("Old Stock price is = " + oldStockPrice);
					//System.out.println("Old Stock volume is = " + oldStockVolume);
					//System.out.println("New Stock price is = " + newStockPrice);
					//System.out.println("New Stock volume is = " + newStockVolume);
				}
			} else {
				//System.out.println("Executing generateData() first time...");
				newStockPrice = Double.parseDouble(basePrice);
				newStockVolume = 0;
				//System.out.println("Stock price is = " + newStockPrice);
				//System.out.println("Stock volume is = " + newStockVolume);

			}

			sb.append("{\"" + stringsn + "\":\"" + key + "\" , ")
					.append("\"" + stringst + "\" : \"" + StockTimestamp + "\" , ")
					.append("\"" + stringsp + "\" : " + Double.toString(newStockPrice) + " , ")
					.append("\"" + stringsv + "\" : " + Integer.toString(newStockVolume) + " } ");

			//System.out.println("Key = " + key);
			System.out.println("Value = " + sb.toString());

			output.put(key, sb.toString());

		}
		// putKafka();
	}

	public void updateProp() {
		StringBuilder sb = new StringBuilder();
		String finalPropValue;
		for (Map.Entry<String, String> entry : output.entrySet()) {
			//System.out.println(entry.getValue());
			sb.append(entry.getKey()).append(":").append(entry.getValue().split(",")[2].split(":")[1].trim())
					.append(",");
		}
		// //System.out.println(sb);
		// //System.out.println(sb.length());
		finalPropValue = sb.substring(0, sb.length() - 1);

		// //System.out.println(finalPropValue);
		stockProp.setProperty(propStockDetails, finalPropValue);
		// //System.out.println(stockProp.getProperty(propStockDetails));

		try {

			FileOutputStream fos = new FileOutputStream(file);
			stockProp.save(fos, "");
			// prop.store(out, null);
			fos.close();
			//System.out.println("property value updated.");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void setWrongDataPos() {
		wrongDataFlag = 1;
		wrongDataPos = rand.nextInt(300);
		if (wrongDataPos < 200)
			wrongDataPos = wrongDataPos + (200 - wrongDataPos);

		System.out.println("Code will generate wrong data on position = " + wrongDataPos);
	}

	public void run() {

		//System.out.println("********** Inside Run **********");

		if (firstExecution == 0) {
			//System.out.println("First time executing code...");
			//System.out.println("Initialzing values...");
			//System.out.println("Loading properties...");
			loadProp();
			//System.out.println("Initialzing stock symbols...");
			getStocks();
			//System.out.println("Initialzing kafka producer...");
			initializeKafka();
			firstExecution = 1;
		}
		long diff = currEndDate.getTime() - currStrtDate.getTime();
		long diffSeconds = diff / 1000;

		for (int i = 0; i <= diffSeconds; i++) {
			if (i == 0) {
				StockTimestamp = new Timestamp(currStrtDate.getTime());
			} else {
				cal.setTimeInMillis(StockTimestamp.getTime());
				cal.add(Calendar.SECOND, 1);
				StockTimestamp = new Timestamp(cal.getTime().getTime());
			}
			//System.out.println(StockTimestamp);
			generateData();
			// putKafka();

		}
		updateProp();
	}

	public static void main(String[] args) {
		//System.out.println("Inside main method of public class.");
		DataForGivenTimeFrame obj = new DataForGivenTimeFrame();
		//System.out.println("Initialzing dates...");
		obj.setCurrentDate(args[0], args[1]);
		//System.out.println("Checking wrong data parameter...");
		if (Integer.parseInt(args[2]) == 1)
			obj.setWrongDataPos();
		obj.run();
	}
}
