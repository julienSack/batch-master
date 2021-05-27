package com.ipiecoles.batch.csvImport;

import com.ipiecoles.batch.model.Commune;
import com.ipiecoles.batch.utils.OpenStreetMapUtils;
import org.springframework.batch.item.ItemProcessor;

import java.util.Map;

public class CommunesMissingCoordinatesItemProcessor implements ItemProcessor<Commune, Commune> {
    @Override
    public Commune process(Commune item) throws Exception {
        Map<String, Double> coordinates0SM = OpenStreetMapUtils.getInstance().getCoordinates(
                item.getNom() + " " + item.getCodePostal());
        if (coordinates0SM != null && coordinates0SM.size() == 2){
            item.setLongitude(coordinates0SM.get("lon"));
            item.setLatitude(coordinates0SM.get("lat"));
            return item;
        }
        //si les données n'ont pas été trouvé on ne renvoie rien - meilleure performance.
        return null;

    }
}
