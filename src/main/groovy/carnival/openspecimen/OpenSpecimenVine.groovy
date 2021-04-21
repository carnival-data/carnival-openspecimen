package carnival.openspecimen



import groovy.transform.ToString
import groovy.transform.EqualsAndHashCode
import groovy.util.logging.Slf4j

import javax.inject.Singleton
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy
import java.text.DateFormat
import javax.inject.Inject
import java.net.URLEncoder

import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.client.RxStreamingHttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.context.annotation.Property


import carnival.core.vine.Vine
import carnival.core.vine.JsonVineMethod



@ToString(includeNames=true)
@Slf4j 
@Singleton
abstract class OpenSpecimenVine implements Vine {


    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    /** Used for authentication */
    abstract String getUsername()

    /** Used for authentication */
    abstract String getPassword()

    /** It is left to the subclass to provide the HTTP client */
    abstract RxStreamingHttpClient getClient()
    //abstract void setClient(RxStreamingHttpClient client)



    ///////////////////////////////////////////////////////////////////////////
    // UTILITY METHODS
    ///////////////////////////////////////////////////////////////////////////

    /** Returns the auth string given the credentials */
    String usernamePassword() { "${username}:${password}" }


    /** Returns the basic-auth string to pass with requests */
    String basicUrlAuthentication() {
		"Basic "+ Base64.getEncoder().encodeToString(usernamePassword().getBytes());        
    }


    ///////////////////////////////////////////////////////////////////////////
    // VINE METHODS
    ///////////////////////////////////////////////////////////////////////////

    @Slf4j
    class CollectionProtocols extends JsonVineMethod<List<CollectionProtocol>> {
		List<CollectionProtocol> fetch(Map args = [:]) {
            log.trace "CollectionProtocols args:${args}"
            HttpRequest req = HttpRequest.GET("/openspecimen/rest/ng/collection-protocols")
            req.headers.add("Authorization", basicUrlAuthentication())
            List<CollectionProtocol> cps = client.jsonStream(req, CollectionProtocol.class).toList().blockingGet()
            log.trace "cps(${cps.size()}): ${cps.take(2)}..."
            return cps
        }
    }


    @Slf4j
    class CollectionProtocolEvents extends JsonVineMethod<List<CollectionProtocolEvent>> {
		List<CollectionProtocolEvent> fetch(Map args = [:]) {
            log.trace "CollectionProtocolEvents args:${args}"
            assert args.cpId
            HttpRequest req = HttpRequest.GET("/openspecimen/rest/ng/collection-protocol-events")
            req.parameters.add('cpId', String.valueOf(args.cpId))
            req.headers.add("Authorization", basicUrlAuthentication())
            List<CollectionProtocolEvent> objs = client.jsonStream(req, CollectionProtocolEvent.class).toList().blockingGet()
            log.trace "objs(${objs.size()}): ${objs.take(2)}..."
            return objs
        }
    }


    @Slf4j
    class SpecimenRequirements extends JsonVineMethod<List<SpecimenRequirement>> {
		List<SpecimenRequirement> fetch(Map args = [:]) {
            log.trace "SpecimenRequirements args:${args}"
            assert args.cpId
            assert args.eventLabel
            boolean includeChildReqs = args.includeChildReqs ?: false

            HttpRequest req = HttpRequest.GET("/openspecimen/rest/ng/specimen-requirements")
            req.headers.add("Authorization", basicUrlAuthentication())
            req.parameters.add('cpId', String.valueOf(args.cpId))
            req.parameters.add('eventLabel', String.valueOf(args.eventLabel))
            req.parameters.add('includeChildReqs', String.valueOf(includeChildReqs))
            List<SpecimenRequirement> objs = client.jsonStream(req, SpecimenRequirement.class).toList().blockingGet()
            log.trace "objs(${objs.size()}): ${objs.take(2)}..."
            return objs
        }
    }


    @Slf4j
    class CollectionProtocolRegistrations extends JsonVineMethod<List<CollectionProtocolRegistration>> {
		List<CollectionProtocolRegistration> fetch(Map args = [:]) {
            log.trace "CollectionProtocolRegistrations args:${args}"
            assert args.cpId

            Map body = [
                cpId:args.cpId,
                startAt:0,
                maxResults:100
            ]

            List<CollectionProtocolRegistration> allCprs = new ArrayList<CollectionProtocolRegistration>()
            boolean moreToFetch = true

            while (moreToFetch) {
                log.trace "CollectionProtocolRegistrations ${body}"
                HttpRequest req = HttpRequest.POST(
                    "/openspecimen/rest/ng/collection-protocol-registrations/list",
                    body
                )
                req.headers.add("Authorization", basicUrlAuthentication())
                List<CollectionProtocolRegistration> cps = client.jsonStream(req, CollectionProtocolRegistration.class).toList().blockingGet()
                log.trace "cps(${cps.size()}): ${cps.take(2)}..."

                allCprs.addAll(cps)
                moreToFetch = cps.size() == 100
                body.startAt += cps.size()
            }

            return allCprs
        }
    }


    ///////////////////////////////////////////////////////////////////////////
    // MODEL
    ///////////////////////////////////////////////////////////////////////////

    @ToString(includeNames=true)
    static class CollectionProtocol {
        Integer id
        String title
        String shortTitle
        String code
        Integer participantCount
        Integer specimenCount
    }

    @ToString(includeNames=true)
    static class CollectionProtocolEvent {
        Integer id
        String eventLabel
        Integer eventPoint
        String collectionProtocol
        String defaultSite
        String code
    }

    @ToString(includeNames=true)
    static class SpecimenRequirement {
        Integer id
        String name
        String code
        String specimenClass
        String type
        Double initialQuantity
        Double concentration
        String collectionContainer
        String cpShortTitle
        String eventLabel
        String labelFmt
        List<SpecimenRequirement> children
    }

    @ToString(includeNames=true)
    static class CollectionProtocolRegistration {
        Integer id
        Integer cpId
        String cpTitle
        String cpShortTitle
        String ppid
        String activityStatus
        Date registrationDate
        Participant participant
    }

    @ToString(includeNames=true)
    static class Participant {
        Integer id
        String firstName
        String lastName
        String activityStatus
        List<String> registeredCps
    }


    @ToString(includeNames=true)
    static class Visit {
        Integer id
        Integer cprId
        String name
        Date visitDate
        String eventLabel
    }


    @ToString(includeNames=true)
    static class Specimen {
        Integer id
        Integer cpId
        Integer cprId
        Integer eventId
        Integer visitId
        Integer reqId
        Integer parentId
        String label
        String parentLabel
        String type
        String code
        String collectionContainer
    }


    @ToString(includeNames=true)
    static class Aliquot {
        Integer id
        Integer cpId
        Integer cprId
        Integer eventId
        Integer visitId
        Integer reqId
        Integer parentId
        String label
        String parentLabel
        String type
    }

}