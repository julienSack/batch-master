package com.ipiecoles.batch.dbexport;

import com.ipiecoles.batch.repository.CommuneRepository;
import org.springframework.batch.item.file.FlatFileFooterCallback;

import java.io.IOException;
import java.io.Writer;


public class CustomFooter implements FlatFileFooterCallback {

    private final CommuneRepository communeRepository;

    public CustomFooter(CommuneRepository communeRepository) {
        this.communeRepository = communeRepository;
    }

    // Texte qui sera présent en bas du fichier
    @Override
    public void writeFooter(Writer writer) throws IOException {
        writer.write("Total communes : " + communeRepository.countDistinctNom());
    }

}
