package nextflow.openlineage

import groovy.transform.CompileStatic
import nextflow.Session
import nextflow.trace.TraceObserver
import nextflow.trace.TraceObserverFactory
/**
 * Implements the validation observer factory
 *
 * @author Jorge Ejarque <jorge.ejarque@seqera.io>
 */
@CompileStatic
class OpenLineageObserverFactory implements TraceObserverFactory {

    @Override
    Collection<TraceObserver> create(Session session) {
        final enabled = session.config.navigate('openLineage.enabled')
        if (enabled) {
            final server = session.config.navigate('openLineage.server') as String
            if (server) {
                final observers =  new ArrayList<TraceObserver>()
                observers.add(new OpenLineageObserver(server))
                return observers
            } else {
                throw new IllegalArgumentException("OpenLineage API Server must be specified for the nf-open-linage plugin")
            }
        } else {
            return []
        }
    }
}
