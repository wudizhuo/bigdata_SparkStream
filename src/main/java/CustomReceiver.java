import com.opencsv.CSVReader;
import org.apache.ivy.util.StringUtils;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.FileReader;
import java.io.IOException;

public class CustomReceiver extends Receiver<String> {
    public CustomReceiver(StorageLevel storageLevel) {
        super(storageLevel);
    }

    public void onStart() {
        new Thread() {
            @Override
            public void run() {
                receive();
            }
        }.start();
    }

    private void receive() {
        try {
            CSVReader reader = new CSVReader(new FileReader("/home/cloudera/Downloads/Project/survey_results_public.csv"));
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                store(StringUtils.join(nextLine, JavaNetworkWordCount.joinStr));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void onStop() {
    }
}
