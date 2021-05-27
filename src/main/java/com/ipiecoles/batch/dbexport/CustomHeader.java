package com.ipiecoles.batch.dbexport;

import com.ipiecoles.batch.repository.CommuneRepository;
import org.springframework.batch.item.file.FlatFileHeaderCallback;

import java.io.IOException;
import java.io.Writer;

public class CustomHeader implements FlatFileHeaderCallback {

    private final CommuneRepository communeRepository;

    public CustomHeader(CommuneRepository communeRepository) {
        this.communeRepository = communeRepository;
    }

    // Texte qui sera présent pour l'entête du fichier
    @Override
    public void writeHeader(Writer writer) throws IOException {
        writer.write("Total codes postaux : " + communeRepository.countDistinctCodePostal());
    }


}
