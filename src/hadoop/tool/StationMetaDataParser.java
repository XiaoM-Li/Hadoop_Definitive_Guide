package hadoop.tool;

import org.apache.hadoop.io.Text;

public class StationMetaDataParser {

	private String stationID;
	private String stationName;

	public StationMetaDataParser() {

	}

	public void parse(Text record) {
		stationID = record.toString().substring(0, 6) + "-" + record.toString().substring(7, 12);
		stationName = record.toString().substring(13, 43).trim();
	}

	public String getStationID() {
		return stationID;
	}

	public String getStationName() {
		return stationName;
	}

}
