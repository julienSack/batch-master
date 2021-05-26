package com.ipiecoles.batch.dbexport;

import com.ipiecoles.batch.model.Commune;
import com.ipiecoles.batch.repository.CommuneRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.FormatterLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import javax.persistence.EntityManagerFactory;


@Configuration
public class CommunesDBExportBatch {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public EntityManagerFactory entityManagerFactory;

    @Autowired
    private CommuneRepository communeRepository;

    @Value("${importFile.chunkSize}")
    private Integer chunkSize;

    // job export
    @Bean
    @Qualifier("exportCommunes")
    public Job exportCommunes(Step stepExportDB, Step stepExportFile){
        return jobBuilderFactory.get("exportCommunes")
                .incrementer(new RunIdIncrementer())
                .flow(stepExportDB)
                .next(stepExportFile)
                .end().build();
    }



    // Récupere les communes triées par code postal et code insee
    @Bean
    public JpaPagingItemReader<Commune> CommuneReader() {
        return new JpaPagingItemReaderBuilder<Commune>()
                .name("JpaReader")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(10)
                .queryString("from Commune c order by code_postal, code_insee")
                .build();
    }

    // Ecris les résultats dans un fichier text
    @Bean
    public ItemWriter<Commune> fileWriter() {
        BeanWrapperFieldExtractor<Commune> bwfe = new BeanWrapperFieldExtractor<>();
        bwfe.setNames(new String[] {"codePostal", "codeInsee", "nom", "latitude", "longitude"});

        FormatterLineAggregator<Commune> agg = new FormatterLineAggregator<>();
        agg.setFieldExtractor(bwfe);
        agg.setFormat("%5s - %5s - %s : %.5f %.5f");

        FlatFileItemWriter<Commune> flatFileItemWriter = new FlatFileItemWriter<>();
        flatFileItemWriter.setName("txtWriter");
        flatFileItemWriter.setHeaderCallback(new CustomHeader(communeRepository));
        flatFileItemWriter.setFooterCallback(new CustomFooter(communeRepository));
        flatFileItemWriter.setResource(new FileSystemResource("target/test.txt"));
        flatFileItemWriter.setLineAggregator(agg);

        return flatFileItemWriter;
    }

    //listener
    @Bean
    public CommunesDBExportSkipListener communesDBExportSkipListener(){
        return new CommunesDBExportSkipListener();
    }

    //step export DB
    @Bean
    public Step stepExportDB(){
        return stepBuilderFactory.get("stepExportTasklet")
                .tasklet(communesExportTasklet())
                .listener(communesExportTasklet())
                .build();
    }

    // export text
    @Bean
    public Step stepExportFile(){
        return stepBuilderFactory.get("exportFile")
                .<Commune, Commune> chunk(chunkSize)
                .reader(CommuneReader())
                .writer(fileWriter())
                .faultTolerant()
                .skip(FlatFileParseException.class)
                .listener(communesDBExportSkipListener())
                .build();
    }

    // export tasklet
    @Bean
    public Tasklet communesExportTasklet(){ return new CommunesExportTasklet();
    }





}
