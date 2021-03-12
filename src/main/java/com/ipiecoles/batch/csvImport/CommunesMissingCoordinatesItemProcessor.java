package com.ipiecoles.batch.csvImport;

import com.ipiecoles.batch.model.Commune;
import com.ipiecoles.batch.utils.OpenStreetMapUtils;
import org.springframework.batch.item.ItemProcessor;

import java.util.Map;

public class CommunesMissingCoordinatesItemProcessor implements ItemProcessor<Commune, Commune> {
    @Override
    public Commune process(Commune item) throws Exception {
        Map<String, Double> coordinatesOSM = OpenStreetMapUtils.getInstance().getCoordinates(
                item.getNom() + " " + item.getCodePostal());
        if(coordinatesOSM != null && coordinatesOSM.size() == 2){
            item.setLongitude(coordinatesOSM.get("lon"));
            item.setLatitude(coordinatesOSM.get("lat"));
            return item;
        }
        return null;
    }
}
