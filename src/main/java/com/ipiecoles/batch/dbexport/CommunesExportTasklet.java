package com.ipiecoles.batch.dbexport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

public class CommunesExportTasklet implements Tasklet {

    Logger logger = LoggerFactory.getLogger(this.getClass());


    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        System.out.println("Export table DB COMMUNE");
        return RepeatStatus.FINISHED;
    }

    @BeforeStep
    public void beforeStep(StepExecution sExec) throws Exception {
        //Avant l'exécution de la Step
        logger.info("Lancement de l'export de la table COMMUNE en fichier txt");
    }

    @AfterStep
    public ExitStatus afterStep(StepExecution sExec) throws Exception {
        //Une fois la Step
        logger.info("Export de la table COMMUNE terminé");
        logger.info(sExec.getSummary());
        return ExitStatus.COMPLETED;
    }
}
