package com.ipiecoles.batch.csvImport;

import com.ipiecoles.batch.dto.CommuneCSV;
import com.ipiecoles.batch.exception.CommuneCSVException;
import com.ipiecoles.batch.exception.NetworkException;
import com.ipiecoles.batch.model.Commune;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.retry.backoff.FixedBackOffPolicy;


import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class CommunesImportBatch {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public EntityManagerFactory entityManagerFactory;

    // Job principal qui appel les steps définies après
    @Bean
    public Job importCsvJob(Step stepStart, Step stepImportCSV, Step stepGetMissingCoordinates){
        return jobBuilderFactory.get("importCsvJob")
                .incrementer(new RunIdIncrementer())
                .flow(stepStart)
                .next(stepImportCSV)
                .on("COMPLETED_WITH_MISSING_COORDINATES").to(stepGetMissingCoordinates)
                .end().build();
    }

    //*************************************************//
    // STEP 1 : Tasklet Message "Début du traitement" //
    // ***********************************************//

    // Tasklet
    @Bean
    public Tasklet startTasklet(){
        return new StartTasklet();
    }

    // Step
    @Bean
    public Step stepStart() {
        return stepBuilderFactory.get("stepStart")
                .tasklet(startTasklet())
                .listener(startTasklet())
                .build();
    }

    //*************************************//
    // STEP 2 : traitement du fichier CSV //
    //************************************//

    // reader
    @Bean
    public FlatFileItemReader<CommuneCSV> communeCSVItemReader() {
        return new FlatFileItemReaderBuilder<CommuneCSV>()
                .name("communeCSVItemReader")
                .linesToSkip(1)
                .resource(new ClassPathResource("laposte_hexasmal.csv"))
                .delimited()
                .delimiter(";")
                .names("codeInsee", "nom", "codePostal", "ligne5", "libelleAcheminement", "coordonneesGps")
                .fieldSetMapper(new BeanWrapperFieldSetMapper<>() {{
                    setTargetType(CommuneCSV.class);
                }})
                .build();
    }

    // processor
    @Bean
    public CommuneCSVItemProcessor communeCSVItemProcessor(){
        return new CommuneCSVItemProcessor();
    }

    // writer
    @Bean
    public JpaItemWriter<Commune> writerJPA(){
        return new JpaItemWriterBuilder<Commune>().entityManagerFactory(entityManagerFactory)
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<Commune> writerJDBC(DataSource dataSource){
        return new JdbcBatchItemWriterBuilder<Commune>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO COMMUNE(code_insee, nom, code_postal, latitude, longitude) VALUES " +
                        "(:codeInsee, :nom, :codePostal, :latitude, :longitude) " +
                        "ON DUPLICATE KEY UPDATE nom=c.nom, code_postal=c.code_postal, latitude=c.latitude, longitude=c.longitude")
                .dataSource(dataSource).build();
    }

    // listener
    @Bean
    public StepExecutionListener communeCSVImportStepListener(){
        return new CommuneCSVImportStepListener();
    }

    @Bean
    public ChunkListener communeCSVImportChunkListener(){
        return new CommuneCSVImportChunkListener();
    }

    @Bean
    public ItemReadListener<CommuneCSV> communeCSVItemReadListener(){
        return new CommuneCSVItemListener();
    }
    @Bean
    public ItemWriteListener<Commune> communeCSVItemWriteListener(){
        return new CommuneCSVItemListener();
    }

    @Bean
    public CommunesCSVImportSkipListener communesCSVImportSkipListener(){
        return new CommunesCSVImportSkipListener();
    }

    // step
    @Value("${importFile.chunkSize}")
    private Integer chunkSize;

    @Bean
    public Step stepImportCSV(){
        return stepBuilderFactory.get("importFile")
                .<CommuneCSV, Commune> chunk(chunkSize)
                .reader(communeCSVItemReader())
                .processor(communeCSVItemProcessor())
                .writer(writerJPA())
                //.writer(writerJDBC(null)) -- on utilise l'un ou l'autre
                .faultTolerant()
                .skipPolicy(new AlwaysSkipItemSkipPolicy())
                .skip(CommuneCSVException.class)
                .skip(FlatFileParseException.class)
                .listener(communesCSVImportSkipListener())
                //.listener(communeCSVImportStepListener())
                //.listener(communeCSVImportChunkListener())
                //.listener(communeCSVItemReadListener())
                .listener(communeCSVItemWriteListener())
                .listener(communeCSVItemProcessor())
                .build();
    }


    //******************************************************//
    // STEP 3 : traitement pour les coordonnées manquantes //
    //******************************************************//  

    // processor
    @Bean
    public CommunesMissingCoordinatesItemProcessor communesMissingCoordinatesItemProcessor(){
        return new CommunesMissingCoordinatesItemProcessor();
    }

    // reader
    @Bean
    public JpaPagingItemReader<Commune> communesMissingCoordinatesJpaItemReader() {
        return new JpaPagingItemReaderBuilder<Commune>()
                .name("communesMissingCoordinatesJpaItemReader")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(10)
                .queryString("from Commune c where c.latitude is null or c.longitude is null")
                .build();
    }

    // pour le writer on utilise l'existant writerJPA dans le step précédent

    // Step
    @Bean
    public Step stepGetMissingCoordinates(){
        FixedBackOffPolicy policy = new FixedBackOffPolicy();
        policy.setBackOffPeriod(2000);
        return stepBuilderFactory.get("getMissingCoordinates")
                .<Commune, Commune> chunk(10)
                .reader(communesMissingCoordinatesJpaItemReader())
                .processor(communesMissingCoordinatesItemProcessor())
                .writer(writerJPA())
                .faultTolerant()
                .retryLimit(5)
                .retry(NetworkException.class)
                .backOffPolicy(policy)
                .build();
    }


}