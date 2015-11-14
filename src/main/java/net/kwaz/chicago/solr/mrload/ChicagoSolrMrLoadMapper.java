package net.kwaz.chicago.solr.mrload;

import java.io.IOException;

import net.kwaz.chicago.ChicagoKey;
import net.kwaz.chicago.ChicagoValue;
import net.kwaz.chicago.Event;

import org.apache.hadoop.io.Text;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.hadoop.SolrInputDocumentWritable;
import org.apache.solr.hadoop.SolrMapper;
import org.joda.time.format.ISODateTimeFormat;

public class ChicagoSolrMrLoadMapper extends
		SolrMapper<ChicagoKey, ChicagoValue> {

	@Override
	public void map(ChicagoKey key, ChicagoValue value, Context context)
	throws IOException, InterruptedException
	{
		String id = Integer.toString(key.getZipCode())
				+ Long.toString(key.getDateTimeUtc().getMillis());
		Text outKey = new Text(id);
		
		SolrInputDocument sid = new SolrInputDocument();
		
		sid.addField("id", id);
		sid.addField("zipCode_i", key.getZipCode());
		String dateString = ISODateTimeFormat.dateTimeNoMillis().withZoneUTC().print(key.getDateTimeUtc());
		sid.addField("dateTimeUtc_dt", dateString);
		sid.addField("tempF_f", value.getTemperatureFahrenheit());
		sid.addField("dewPointF_f", value.getDewPointFahrenheit());
		sid.addField("humidity_i", value.getHumidity());
		sid.addField("seaLevel_f", value.getSeaLevelInchesOfMercury());
		sid.addField("vis_f", value.getVisibilityMiles());
		sid.addField("windDir_s", value.getWindDirection().name());
		sid.addField("windSpeed_f", value.getWindSpeedMph());
		sid.addField("gustSpeed_f", value.getGustSpeedMph());
		sid.addField("precip_f", value.getPrecipitationInches());
		
		for (Event event : value.getEvents()) {
			sid.addField("events_ss", event.name());
		}
		
		sid.addField("cond_s", value.getConditions());
		sid.addField("windDir_i", value.getWindDirectionDegrees());

		SolrInputDocumentWritable outValue = new SolrInputDocumentWritable(sid);
		context.write(outKey, outValue);
	}
}
