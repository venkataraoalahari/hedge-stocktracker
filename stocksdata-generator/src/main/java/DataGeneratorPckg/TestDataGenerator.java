package DataGeneratorPckg;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

class RunTask extends TimerTask {

	int counter = 0;
	String[] StockList;
	int firstExecution = 0;
	Properties p = new Properties();
	CustomProperties stockProp = new CustomProperties();
	LinkedHashMap<String, String> output = new LinkedHashMap<String, String>();
	public final SimpleDateFormat tmstmpVal = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public final SimpleDateFormat dtVal = new SimpleDateFormat("yyyy-MM-dd");
	public final String startTime = " 08:59:59";
	public final String endTime = " 20:00:01";
	public final String stringsp = "price";
	public final String stringsv = "volume";
	public final String stringst = "timestampValue";
	public final String stringsn = "symbol";
	public final String propStockDetails = "STOCK_DETAILS";
	public final String propFilePath = "app.properties";
	public final String stockPropFilePath = "FILE_PATH";
	public final String kafkaTopic = "TOPIC";
	public final String csvFilePath = "CSV_FILE_PATH";
	public final String acks = "ACKS";
	public final String retries = "RETRIES";
	public final String lingerMs = "LINGER_MS";
	public final String bootstrapServer = "BOOTSTRAP_SERVERS";
	ProducerRecord<String, String> producerRecord;
	DecimalFormat twodeci = new DecimalFormat("0.00");
	int wrongDataPos = 0;
	int wrongDataFlag = 0;
	int wrongDataCounter = 0;
	String oldkeyforWrongData = "";
	double oldPriceBeforeWrongData = 0;
	int oldVolumeBeforeWrongData = 0;
	int flagTimeRange = 0;
	FileWriter writer;

	Properties kafkaProp = new Properties();
	Producer<String, String> producer;
	Timestamp StockTimestamp;
	Timestamp currStrtDate;
	Timestamp currEndDate;
	Random rand = new Random();
	Calendar cal = Calendar.getInstance();
	Number n = 0;

	DecimalFormat df = new DecimalFormat("#.####");


	File file;

	public RunTask(String[] args) {
		try {
			if (args.length == 3) {
				System.out.println("Program called with time frame and wrong data flag.");
				flagTimeRange = 1;
				setCurrentDate(args[0], args[1]);
				StockTimestamp = new Timestamp(currStrtDate.getTime());
				// System.out.println("Checking wrong data parameter...");
				if (Integer.parseInt(args[2]) == 1)
					setWrongDataPos();
			} else if (args.length == 1) {
				System.out.println("Program called with only wrong data flag. System dates will be used for timestamp.");
				if (Integer.parseInt(args[0]) == 1)
					setWrongDataPos();
				setCurrentDate("", "");
			} else {
				System.out.println("Program called without any parameters.");
				setCurrentDate("", "");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void loadProp() {
		try {
			p.load(TestDataGenerator.class.getClassLoader().getResourceAsStream(propFilePath));
			file = new File(p.getProperty(stockPropFilePath));
			FileInputStream fileInput = new FileInputStream(file);
			stockProp.load(fileInput);
			fileInput.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void setCurrentDate(String strtDt, String endDt) {
		if (flagTimeRange == 1 && !strtDt.equals("") && !endDt.equals("")) {
			System.out.println("Setting user provided start time and end time.");
			// System.out.println("First argument is = " + strtDt + "\t Second argument is = " + endDt);
			currStrtDate = Timestamp.valueOf(strtDt);
			currEndDate = Timestamp.valueOf(endDt);
			StockTimestamp = new Timestamp(currStrtDate.getTime());
			// System.out.println("Start time is = " + currStrtDate + "\t Endtime is = " + currEndDate);
			System.out.println("Start time is = " + currStrtDate + "\t End time is = " + currEndDate
					+ "\t Current timestamp is = " + StockTimestamp);
		} else {
			System.out.println("Setting system generated start time and end time.");
			try {
				currStrtDate = new Timestamp(tmstmpVal.parse(dtVal.format(new Date()) + startTime).getTime());
				currEndDate = new Timestamp(tmstmpVal.parse(dtVal.format(new Date()) + endTime).getTime());
				
				Calendar cal1 = Calendar.getInstance();
				cal1.set(Calendar.MILLISECOND, 0);
				StockTimestamp = new Timestamp(cal1.getTimeInMillis());
				System.out.println("Start time is = " + currStrtDate + "\t End time is = " + currEndDate
						+ "\t Current timestamp is = " + StockTimestamp);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public String getProp(String key) {
		return stockProp.getProperty(key);
	}

	public void getStocks() {
		StockList = stockProp.getProperty(propStockDetails).split(",");
	}

	public void putKafka() {
		// System.out.println("Loading data into kafka topic....");
		Iterator<String> it = output.keySet().iterator();
		while (it.hasNext()) {
			String key = (String) it.next();
			String value = output.get(key);
			// System.out.println("kafka key = " + key + " \t kafka value = " +
			// value);
			producerRecord = new ProducerRecord<String, String>(getProp(kafkaTopic), key, value);
			producer.send(producerRecord);
		}
		// System.out.println("Loading data into kafka is finished.");
	}

	public void initializeKafka() {
		kafkaProp.setProperty("bootstrap.servers", getProp(bootstrapServer));
		kafkaProp.setProperty("key.serializer", StringSerializer.class.getName());
		kafkaProp.setProperty("value.serializer", StringSerializer.class.getName());
		// producer acks
		kafkaProp.setProperty("acks", getProp(acks));
		kafkaProp.setProperty("retries", getProp(retries));
		kafkaProp.setProperty("linger.ms", getProp(lingerMs));

		producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(kafkaProp);

	}

	public void generateData() {
		float randomVal = 0;
		String key, basePrice;
		double newStockPrice = 0, oldStockPrice = 0;
		int randomVolume = 0, newStockVolume = 0, oldStockVolume = 0;
		StringBuilder sb = new StringBuilder();
		// System.out.println("Total no. of stocks are | " + StockList.length);

		for (int i = 0; i < StockList.length; i++) {

			// System.out.println(StockList[i]);
			sb.delete(0, sb.length());
			key = StockList[i].split(":")[0];
			// System.out.println(key);
			basePrice = StockList[i].split(":")[1];
			// System.out.println("Base Price for stock " + key + " is = "
			// +basePrice);

			if (wrongDataFlag == 1) {
				wrongDataCounter++;
				if (wrongDataCounter == wrongDataPos) {
					// System.out.println("Inside wrong data generation
					// part...");
					randomVal = (rand.nextFloat() * (rand.nextBoolean() ? -1 : 1)) / 10;
					randomVolume = rand.nextInt(3) + 1;
					// System.out.println("Random float value is = " +
					// randomVal);
					// System.out.println("Random Int volumn is = " +
					// randomVolume);
				} else {
					// System.out.println("Inside correct data generation
					// part...");
					randomVal = (rand.nextFloat() * (rand.nextBoolean() ? -1 : 1)) / 100;
					randomVolume = rand.nextInt(3) + 1;
					// System.out.println("Random float value is = " +
					// randomVal);
					// System.out.println("Random Int volumn is = " +
					// randomVolume);
				}
			}else{
				// System.out.println("Inside correct data generation part...");
				randomVal = (rand.nextFloat() * (rand.nextBoolean() ? -1 : 1)) / 100;
				randomVolume = rand.nextInt(3) + 1;
				// System.out.println("Random float value is = " +
				// randomVal);
				// System.out.println("Random Int volumn is = " +
				// randomVolume);
				
			}

			if (output.containsKey(key)) {
				// System.out.println("Stock already exists...");
				if (wrongDataFlag == 1 && wrongDataCounter == wrongDataPos) {
					oldkeyforWrongData = key;
					System.out.println(key);
					System.out.println("Generating wrong data for key = " + oldkeyforWrongData);
					oldPriceBeforeWrongData = Double.parseDouble(output.get(key).split(",")[2].split(":")[1].trim());
					oldVolumeBeforeWrongData = Integer
							.parseInt(output.get(key).split(",")[3].split(":")[1].replace("}", "").trim());
					newStockPrice = oldPriceBeforeWrongData + (oldPriceBeforeWrongData * randomVal);
					newStockVolume = oldVolumeBeforeWrongData + randomVolume;
					// System.out.println("Old Stock price is = " + oldPriceBeforeWrongData);
					// System.out.println("Old Stock volume is = " + oldVolumeBeforeWrongData);
					// System.out.println("New Stock price is = " + newStockPrice);
					// System.out.println("New Stock volume is = " + newStockVolume);
					setWrongDataPos();
				} else if (wrongDataFlag == 1 && oldkeyforWrongData.equals(key)) {
					System.out.println("Generating correct data after wrong data is generated.");
					oldStockPrice = oldPriceBeforeWrongData;
					oldStockVolume = oldVolumeBeforeWrongData;
					newStockPrice = oldStockPrice + (oldStockPrice * randomVal);
					newStockVolume = oldStockVolume + randomVolume;
					oldkeyforWrongData = "";
					// System.out.println("Old Stock price is = " + oldStockPrice);
					// System.out.println("Old Stock volume is = " + oldStockVolume);
					// System.out.println("New Stock price is = " + newStockPrice);
					// System.out.println("New Stock volume is = " + newStockVolume);

				} else {
					// System.out.println("Generating correct data.");
					oldStockPrice = Double.parseDouble(output.get(key).split(",")[2].split(":")[1].trim());
					oldStockVolume = Integer
							.parseInt(output.get(key).split(",")[3].split(":")[1].replace("}", "").trim());
					newStockPrice = oldStockPrice + (oldStockPrice * randomVal);
					newStockVolume = oldStockVolume + randomVolume;
					// System.out.println("Old Stock price is = " + oldStockPrice);
					// System.out.println("Old Stock volume is = " + oldStockVolume);
					// System.out.println("New Stock price is = " + newStockPrice);
					// System.out.println("New Stock volume is = " + newStockVolume);
				}
			} else {
				// System.out.println("Executing generateData() first time...");
				newStockPrice = Double.parseDouble(basePrice);
				newStockVolume = 0;
				// System.out.println("Stock price is = " + newStockPrice);
				// System.out.println("Stock volume is = " + newStockVolume);

			}

			sb.append("{\"" + stringsn + "\":\"" + key + "\" , ")
					.append("\"" + stringst + "\" : \"" + StockTimestamp + "\" , ")
					.append("\"" + stringsp + "\" : " + df.format((Number)newStockPrice) + " , ")
					.append("\"" + stringsv + "\" : " + Integer.toString(newStockVolume) + " } ");

			// System.out.println("Key = " + key);
			System.out.println("Value = " + sb.toString());

			output.put(key, sb.toString());

		}
		// putKafka();
	}

	public void updateProp() {
		StringBuilder sb = new StringBuilder();
		String finalPropValue;
		for (Map.Entry<String, String> entry : output.entrySet()) {
			// System.out.println(entry.getValue());
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
			stockProp.store(fos, "");
			// prop.store(out, null);
			fos.close();
			// System.out.println("property value updated.");
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
	
	public void writeToCsv(){
		//System.out.println("Writing stock details to csv file...");
		StringBuilder sb = new StringBuilder();
		Iterator<String> it = output.keySet().iterator();
		while (it.hasNext()) {
			String key = (String) it.next();
			String value = output.get(key);
			sb.append(value.split(",")[0].split(":")[1].trim())
			  .append(",")
			  .append(value.split(",")[1].replaceFirst(":", "%%").split("%%")[1].trim())
			  .append(",")
			  .append(value.split(",")[2].split(":")[1].trim())
			  .append(",")
			  .append(value.split(",")[3].split(":")[1].replace("}", "").trim())
			  .append('\n');
			
			//System.out.println(sb);
			
			try {
				writer.append(sb);
				writer.flush();
				sb.delete(0, sb.length());
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
		System.out.println("csv file is appended.");
	}

	public void intializeCsvDetails(){
		try{
			writer = new FileWriter(p.getProperty(csvFilePath),true);
			StringBuilder sb = new StringBuilder();
			if(new File(getProp(csvFilePath)).length() == 0){
				sb.append("Symbol")
				  .append(",")
				  .append("Timestamp")
				  .append(",")
				  .append("price")
				  .append(",")
				  .append("volume")
				  .append('\n');
				writer.append(sb);
				sb.delete(0, sb.length());
			}
			//if( !writer.exists() || (writer.length() == 0))
		}catch(Exception e){
			System.out.println(e);
		}
	}
	
	public void run() {

		// System.out.println("********** Inside Run **********");

		if (firstExecution == 0) {
			// System.out.println("First time executing code...");
			// System.out.println("Initializing values...");
			// System.out.println("Loading properties...");
			loadProp();
			// System.out.println("Initializing stock symbols...");
			getStocks();
			// System.out.println("Initializing kafka producer...");
			initializeKafka();
			// System.out.println("Initializing Output file parameters...");
			intializeCsvDetails();
			df.setRoundingMode(RoundingMode.CEILING);	
			firstExecution = 1;
		}
			Date currTime = new Date(StockTimestamp.getTime());
			System.out.println("******************");
			System.out.println(currTime);
			System.out.println(currTime.compareTo(currStrtDate));
			System.out.println(currTime.compareTo(currEndDate));
			System.out.println("******************");
			// Date currTime = tmstmpVal.parse(StockTimestamp);
			if(currTime.compareTo(currStrtDate) == 1 && currTime.compareTo(currEndDate) ==1 ){
				System.out.println("Current Time is outside given time range.");
				updateProp();
				try {
					writer.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.exit(-1);
			}
			else{
				System.out.println("Current Time is inbetween given time range.");
				System.out.println("calling generateData....");
				generateData();
				putKafka();
				//writeToCsv();
				cal.setTimeInMillis(StockTimestamp.getTime());
				cal.add(Calendar.SECOND, 1);
				StockTimestamp = new Timestamp(cal.getTime().getTime());
			}
			/*if ((flagTimeRange == 1) || ((currTime.after(currStrtDate)) && (currTime.before(currEndDate)))) {
				System.out.println("Current Time is inbetween given time range.");
				System.out.println("calling generateData....");
				generateData();
				updateProp();
				cal.setTimeInMillis(StockTimestamp.getTime());
				cal.add(Calendar.SECOND, 1);
				StockTimestamp = new Timestamp(cal.getTime().getTime());
			} else {
				System.out.println("Current TIme is outside given time range.");
				System.exit(-1);
			}*/
		//updateProp();
	}

}

public class TestDataGenerator {

	public static void main(String[] args) {
		//System.out.println("Inside main method of public class.");
		for(int i = 0 ; i < args.length ; i++) {
			System.out.println(args[i]);
		}
		if (!(args.length == 3 || args.length == 1 || args.length == 0)){
			System.out.println("Program called with wrong no. of arguments. Expected parameters are 3 or 1 or 0");
			System.out.println("Call program with Time frame and wrong data flag. eg.  \"2018-04-20 09:00:00\" \"2018-04-20 09:01:00\" 1");
			System.out.println("Call program with only wrong data flag. eg. 1");
			System.out.println("or Call program without any prameters.");
			System.exit(-1);
		}
		TimerTask task = new RunTask(args);
		Timer timer = new Timer();
		timer.schedule(task, 0, 1000);
	}
}
