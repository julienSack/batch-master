package com.ipiecoles.batch.csvImport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

public class CommuneCSVImportStepListener implements StepExecutionListener {

    Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void beforeStep(StepExecution stepExecution) {
        //Avant l'exécution de la Step
        logger.info("Before Step CVS import");

    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        //Une fois la Step exécutée
        logger.info("After Step CVS import");
        logger.info(stepExecution.getSummary());
        return ExitStatus.COMPLETED;
    }
}
