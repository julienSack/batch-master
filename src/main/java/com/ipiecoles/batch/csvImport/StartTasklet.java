package com.ipiecoles.batch.csvImport;

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

public class StartTasklet implements Tasklet {

    Logger logger = LoggerFactory.getLogger(this.getClass());

    private String message = null;

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        System.out.println("Début du traitement du fichier");
        // Transmettre le message à la step CSVImport
        message = "Le traitement est fini... regardons les lignes avec les coordonnées manquantes :";
        return RepeatStatus.FINISHED;
    }

    @BeforeStep
    public void beforeStep(StepExecution sExec) throws Exception {
        //Avant l'exécution de la Step
        logger.info("Before Tasklet Message");
    }

    @AfterStep
    public ExitStatus afterStep(StepExecution sExec) throws Exception {
        //Une fois la Step exécutée
        sExec.getJobExecution().getExecutionContext().put("MSG", message);
        logger.info("After Tasklet Message");
        logger.info(sExec.getSummary());
        return ExitStatus.COMPLETED;
    }
}
