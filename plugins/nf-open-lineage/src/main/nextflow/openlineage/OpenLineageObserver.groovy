package nextflow.openlineage

import com.google.common.hash.HashCode
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovyx.gpars.extra166y.Ops.Op
import io.openlineage.client.OpenLineage
import io.openlineage.client.OpenLineageClient
import io.openlineage.client.transports.HttpTransport
import nextflow.Session
import nextflow.processor.TaskHandler
import nextflow.processor.TaskRun
import nextflow.trace.TraceObserver
import nextflow.trace.TraceRecord
import java.nio.file.Path
import java.time.ZonedDateTime
import java.util.concurrent.ConcurrentHashMap

/**
 * OpenLineage events observer
 *
 * @author Jorge Ejarque <jorge.ejarque@seqera.io>
 */
@Slf4j
@CompileStatic
class OpenLineageObserver implements TraceObserver {
    private OpenLineageClient client
    private static OpenLineage OL = new OpenLineage(URI.create("nextflow"))
    private OpenLineage.Job workflow
    private OpenLineage.Run workflowRun
    private boolean started = false

    OpenLineageObserver(String server){
        this.client = OpenLineageClient.builder()
                .transport(HttpTransport.builder().uri(server).build())
                .build();
    }

    @Override
    void onFlowCreate(Session session) {
        final namespace = session.workflowMetadata.repository ? session.workflowMetadata.repository : ""
        def name = session.workflowMetadata.manifest ? session.workflowMetadata.manifest.name : session.workflowMetadata.scriptFile.name
        name = name ? name : "main.nf"
        final workflowType = OL.newJobTypeJobFacet("BATCH", "Nextflow", "DAG")
        this.workflow = OL.newJobBuilder().namespace(namespace).name(name)
                .facets(OL.newJobFacetsBuilder().jobType(workflowType).build())
                .build()
        this.workflowRun = OL.newRunBuilder().runId(session.uniqueId).build()
        log.debug("[Openlineage] Workflow $namespace.$name job and run created.")
    }

    @Override
    void onFlowComplete() {
        if (workflowRun && started) {
            final event = OL.newRunEventBuilder()
                    .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
                    .eventTime(ZonedDateTime.now()).job(workflow).run(workflowRun)
                    .build()
            client.emit(event)
            log.debug("[Openlineage] Workflow complete event sent.")
        }
    }

    @Override
    void onFlowBegin(){
        final event = OL.newRunEventBuilder().eventTime(ZonedDateTime.now())
                .eventType(OpenLineage.RunEvent.EventType.START)
                .run(this.workflowRun).job(this.workflow)
                .build()
        client.emit(event)
        started = true
        log.debug("[Openlineage] Workflow start event sent.")
    }

    void onProcessPending(TaskHandler var1, TraceRecord var2){

    }

    void onProcessSubmit(TaskHandler th, TraceRecord var2){

    }

    @Override
    void onProcessStart(TaskHandler handler, TraceRecord trace){
        final job = generateTaskJob(handler.task)
        final jobRun = generateInitialTaskRun(handler.task)
        final inputs = getInputsFromTask(handler.task)
        final event = OL.newRunEventBuilder()
                .eventType(OpenLineage.RunEvent.EventType.START)
                .eventTime(ZonedDateTime.now()).job(job).run(jobRun).inputs(inputs).build()
        client.emit(event)
        log.debug("[Openlineage] Task ${handler.task.id} start event sent.")
    }

    private OpenLineage.Job generateTaskJob(TaskRun task){
        final jobType = OL.newJobTypeJobFacet("BATCH", "Nextflow", "JOB")
        return OL.newJobBuilder().namespace(this.workflow.namespace).name(task.name)
                .facets(OL.newJobFacetsBuilder().jobType(jobType).build())
                .build()
    }

    private OpenLineage.Run generateInitialTaskRun(TaskRun task){
        final parentRun =  generateTaskParent(task)
        final descFacet = generateRunFacet([hashcode: task.hash.toString(), id: task.id])
        final jobRun = OL.newRun(UUID.nameUUIDFromBytes(task.hash.asBytes()),
                OL.newRunFacetsBuilder().parent(parentRun).put("description", descFacet).build())
        return jobRun
    }
    private OpenLineage.ParentRunFacet generateTaskParent(TaskRun task) {
        return OL.newParentRunFacet(OL.newParentRunFacetRun(this.workflowRun.getRunId()),
                OL.newParentRunFacetJob(this.workflow.namespace, this.workflow.name))
    }

    private static List<OpenLineage.InputDataset> getInputsFromTask(TaskRun task){
        final inputs = new LinkedList<OpenLineage.InputDataset>()
        task.getInputFilesMap().each{ name,path -> inputs.add(OL.newInputDatasetBuilder()
                .namespace(path.toFile().parent).name(path.toFile().name).build()) }
        return inputs
    }

    @Override
    void onProcessComplete(TaskHandler handler, TraceRecord var2){
        final job = generateTaskJob(handler.task)
        final parent = generateTaskParent(handler.task)
        def event
        if( handler.task.isSuccess() ){
            event = generateSuccessEvent(handler.task, job, parent)
        } else {
            event = generateFailEvent(handler.task, job, parent)

        }
        client.emit(event)
        log.debug("[Openlineage] Task ${handler.task.id} complete event sent.")
    }
    private static OpenLineage.RunEvent generateSuccessEvent(TaskRun task, OpenLineage.Job job, OpenLineage.ParentRunFacet parent){
        final jobRun = OL.newRunBuilder().runId(UUID.nameUUIDFromBytes(task.hash.asBytes()))
                .facets(OL.newRunFacetsBuilder().parent(parent).build())
                .build()
        final outputs = getOutputsFromTask(task)
        final event = OL.newRunEventBuilder()
                .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
                .eventTime(ZonedDateTime.now())
                .job(job).run(jobRun).outputs(outputs)
                .build()
    }

    private static OpenLineage.RunEvent generateFailEvent(TaskRun task, OpenLineage.Job job, OpenLineage.ParentRunFacet parent){

        def err = task.dumpStderr()
        err = err ? err.join('\n'): ""
        final jobRun = OL.newRunBuilder().runId(UUID.nameUUIDFromBytes(task.hash.asBytes()))
                .facets(OL.newRunFacetsBuilder()
                        .errorMessage(OL.newErrorMessageRunFacet("Task $task.name failed", "Nextflow", err))
                        .parent(parent)
                        .build())
                .build()
        final event = OL.newRunEventBuilder()
                .eventType(OpenLineage.RunEvent.EventType.FAIL)
                .eventTime(ZonedDateTime.now()).job(job).run(jobRun)
                .build()
    }

    private static OpenLineage.RunEvent generateCachedEvent(TaskRun task, OpenLineage.Job job, OpenLineage.ParentRunFacet parent){
        final jobRun = OL.newRunBuilder().runId(UUID.nameUUIDFromBytes(task.hash.asBytes()))
                .facets(OL.newRunFacetsBuilder().parent(parent).put("abort", generateRunFacet([reason: "cached"])).build())
                .build()
        final event = OL.newRunEventBuilder()
                .eventType(OpenLineage.RunEvent.EventType.ABORT)
                .eventTime(ZonedDateTime.now()).job(job).run(jobRun)
                .build()
    }

    private static OpenLineage.RunFacet generateRunFacet(Map props){
        OpenLineage.RunFacet facet = OL.newRunFacet()
        props.each {facet.getAdditionalProperties().put(it.key.toString(), it.value)}
        return facet
    }


    private static List<OpenLineage.OutputDataset> getOutputsFromTask(TaskRun task){
        final outputs = new LinkedList<OpenLineage.OutputDataset>()
        task.getOutputFilesNames().each{
            outputs.add(OL.newOutputDatasetBuilder()
                .namespace(task.workDirStr).name(it).build()) }
        return outputs
    }

    @Override
    void onProcessCached(TaskHandler handler, TraceRecord trace){
        final job = generateTaskJob(handler.task)
        final parent = generateTaskParent(handler.task)
        final event = generateCachedEvent(handler.task, job, parent)
        client.emit(event)
        log.debug("[Openlineage] Task ${handler.task.id} cached event sent.")
    }

    @Override
    void onFlowError(TaskHandler var1, TraceRecord var2){
        if (workflowRun && started) {
            final event = OL.newRunEventBuilder()
                    .eventType(OpenLineage.RunEvent.EventType.FAIL)
                    .eventTime(ZonedDateTime.now()).job(workflow).run(workflowRun)
                    .build()
            client.emit(event)
            log.debug("[Openlineage] Workflow error event sent.")
        }
    }

    @Override
    void onWorkflowPublish(Object var1){

    }

    @Override
    void onFilePublish(Path var1){

    }

    @Override
    void onFilePublish(Path var1, Path var2){

    }
}