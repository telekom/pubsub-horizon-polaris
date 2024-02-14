# Architecture
Polaris serves as Horizon's circuit breaker microservice, integral to the reliable delivery of events. 
Its primary function involves periodic checks of a customer's endpoint, employing HEAD or GET requests to determine availability. 
Upon endpoint recovery, Polaris orchestrates the redelivery of failed events linked to that endpoint. 
This interaction is achieved through sharing a Hazelcast cache with Comet, aptly named the circuit breaker cache.

When Comet faces difficulties delivering an event to a customer, it marks the event as waiting and opens a circuit breaker. 
Polaris polls this cache at regular intervals to identify entries with an 'OPEN' circuit breaker status. 
Following this identification, Polaris hashes the associated callback URL and matches it with the number of running pods in the cluster.

To ensure exclusive handling, Polaris utilizes a modulo operation, comparing the hash with its pod index. 
This mechanism guarantees that only the designated pod processes the circuit breaker message, preventing duplication or conflicting actions by other pods. 
Furthermore, Polaris checks for prior assignments to avoid handling messages already claimed by other pods.

Once assigned, Polaris transitions the circuit breaker status from 'OPEN' to 'CHECKING'. 
The subsequent step involves a Subscription check, examining the current subscription against the circuit breaker message. 
This verification considers parameters such as callback URL, delivery type, HTTP method, and the circuit-breaker-opt-out flag.

Alterations in these parameters trigger specific actions by Polaris.
Certainly, let's refine and clarify the explanation:

**Callback URL Change:**
If the callback URL changes, it indicates that the old endpoint is no longer valid. Consequently, events should be directed to a new endpoint, and the health request, which determines endpoint availability, should be adjusted accordingly.

**Delivery Types:**
- **Callback (CALLBACK):**
  This means that our system, Horizon, will directly send the event over HTTPS to the customer. This responsibility lies with Comet.
- **Server-Sent Events (SSE):**
  In this case, the event is not sent directly to the customer. Instead, the customer initiates a call to a REST endpoint to receive all its open events.

**Handling Delivery Type Changes:**
A change in delivery type signifies that events should either no longer be redelivered or delivered for the first time.

When transitioning from CALLBACK to SSE:
Polaris updates the status of all WAITING events to PROCESSED and changes the delivery type in these events to CALLBACK.

When transitioning from SSE to CALLBACK:
Polaris maintains the event status as PROCESSED but resets the delivery type for each event to CALLBACK.
In both scenarios, Polaris retrieves old events for that event type and republishes them into the subscribed Kafka topic.

**HTTP Method Change:**
If the HTTP method changes, Polaris checks if it's already performing periodic health checks for that callback URL. If so, it stops the existing checks and initiates a new one with the updated HTTP method.

**Circuit Breaker Opt-Out Flag Change:**
If the circuit-breaker-opt-out flag changes from false to true, Polaris initiates the republish process for all WAITING events for that event type and closes the circuit breakers.


Polaris possesses a local health cache containing essential information about endpoint health. 
This cache aids Polaris in making informed decisions when processing incoming circuit breaker messages. 
The key criterion for this cache is the combination of callback URL and HTTP method.

Upon receiving a new circuit breaker message, Polaris checks for an existing health check entry. 
If absent, it creates an entry and initiates a scheduled health request task. 
The scheduling involves a base time with a flexible delay derived from the republish count. 
The flexible delay is calculated as the square of the republish count, capped at a maximum of 60 minutes.

The purpose of this flexible delay is to counteract customer loops. 
In instances where a loop is detected, the health request task's delay increases with each iteration, up to the maximum limit of 60 minutes. 
This strategy introduces a slowdown in the loop, allowing time for endpoint issues to be addressed by the customer.

During the health request task, Polaris performs either a HEAD or GET request to the customer's endpoint. 
A lack of success triggers the initiation of a new scheduled health request task, accompanied by an increase in the republish count. 
Conversely, a successful health check results in the commencement of republishing events associated with the subscription IDs for the callback URL and HTTP method.

In the republishing process, Polaris first sets the status of matching circuit breakers from 'CHECKING' to 'REPUBLISHING.' 
Subsequently, it cleans up the health check cache, retrieves event coordinates from MongoDB, collects events from Kafka, updates their values, and republishes them into the subscribed Kafka topic. 
For cases where an event cannot be retrieved from Kafka, Polaris sends a 'FAILED' status message, indicating the failure with a 'CouldNotPickException.'

This constitutes the primary behavior of Polaris, involving periodic polling of the circuit breaker cache. 
In addition to this, Polaris performs periodic polls on two distinct categories of events. 
First, it polls all events in the database marked as 'DELIVERING' and older than one hour. 
These events are presumed to have encountered issues during delivery. 
Polaris corrects their status, performs a subscription check, and republishes them again.

Second, Polaris periodically polls events in the 'FAILED' status with a 'CallbackException.' 
This category typically arises when Comet experiences internal issues during event delivery, potentially due to a change in the callback URL. 
Here again, Polaris initiates a republishing process for these events.

In addition to these periodic tasks, Polaris subscribes to Subscription changes. 
Consequently, any alteration in a customer's subscription triggers corresponding adjustments in Polaris. 
If changes involve the HTTP method, callback URL, delivery type, or the circuit-breaker-opt-out flag, Polaris adapts its operations accordingly.







````mermaid

flowchart TB
    OnStartup[On Startup]
    OnSubChange[On SubscriptionResource change]
    OnPodDelete[On Pod termination]
    EveryXMinutes[Every X Minutes]
    ForOpenCBs(For each\nOPEN CircuitBreakerMessage)
    ForCheckingCBs(For each\nCHECKING CircuitBreakerMessage)
    ForRepublishingCBs(For each\nREPUBLISHING CircuitBreakerMessage)
    ForAllCBs(For each\n CircuitBreakerMessage)
    ForFAILEDMsgs(For each\nFAILED StatusMessage)
    ForDELIVERINGMsgs(For each\nDELIVERING StatusMessage)

    RepublishTask(RepublishTask)

    CloseCB(Close CircuitBreaker for Msg)
    AssignCBMsgToThisPod(Assign CB-Msg to this pod)
    SetCBMsgToCHECKING(Set CB-Msg to CHECKING)

    isPodIndexZero{Is this pod\nindex == 0?}
    didCallbackUrlChange{Did callback url change?}
    didHttpMethodChange{Did http method change?}
    wasCBOptOutActivated{Was circuit breaker opt-out activated?}
    ShouldCBMsgBeHandledByThisPod{Should CB-Msg be\nhandled by this pod?}
    wasSubscriptionFound{Was fitting Subscription found?}

    RemovePodFromCache(Remove pod from local cache)

    OnStartup --> ForCheckingCBs --> ShouldCBMsgBeHandledByThisPod
    OnStartup --> ForRepublishingCBs --> ShouldCBMsgBeHandledByThisPod
    OnPodDelete --> RemovePodFromCache --> ForAllCBs --> ShouldCBMsgBeHandledByThisPod
    OnSubChange --> |On Delete\n\nSpawn| SubscriptionComparisonTask3[SubscriptionComparisonTask] 
    OnSubChange --> |On Update| didCallbackUrlChange --> |Yes| didHttpMethodChange --> |Yes| wasCBOptOutActivated
    wasCBOptOutActivated --> |Yes\n\nSpawn| SubscriptionComparisonTask2[SubscriptionComparisonTask] 

    EveryXMinutes --> |Read Circuit Breaker Cache| ForOpenCBs
    EveryXMinutes --> |Read FAILED StatusMessages\nwith CallbackUrlNotFoundException| ForFAILEDMsgs
    EveryXMinutes --> |Read DELIVERING StatusMessages| ForDELIVERINGMsgs

    ForOpenCBs --> ShouldCBMsgBeHandledByThisPod
    ForFAILEDMsgs --> isPodIndexZero
    ForDELIVERINGMsgs --> isPodIndexZero
    isPodIndexZero --> |Spawn| RepublishTask

    ShouldCBMsgBeHandledByThisPod --> |Yes| wasSubscriptionFound

    wasSubscriptionFound --> |No| CloseCB
    wasSubscriptionFound --> |Yes| AssignCBMsgToThisPod
    
    AssignCBMsgToThisPod --> SetCBMsgToCHECKING
    SetCBMsgToCHECKING --> |Spawn| SubscriptionComparisonTask

    style SubscriptionComparisonTask stroke:red,stroke-width:2px
    style SubscriptionComparisonTask2 stroke:red,stroke-width:2px
    style SubscriptionComparisonTask3 stroke:red,stroke-width:2px
    style ShouldCBMsgBeHandledByThisPod stroke:cyan,stroke-width:2px
    style RepublishTask stroke:magenta,stroke-width:2px

````
---

### RepublishTask
````mermaid
flowchart TB
    Start
    style Start stroke:magenta,stroke-width:2px
    PickMessagesFromKafka(Pick SubscriptionEventMessages from Kafka)
    SendFAILEDStatusMsgToKafka(Send FAILED StatusMessage to Kafka)
    SendSubEventMsgToKafka(Send SubscriptionEventMessage with PROCESSED to Kafka)

    Start --> PickMessagesFromKafka
    PickMessagesFromKafka --> |Successful| SendSubEventMsgToKafka
    PickMessagesFromKafka --> |Unsuccessful| SendFAILEDStatusMsgToKafka

    SendSubEventMsgToKafka --> |Wait for all| CloseCircuitBreaker
    SendFAILEDStatusMsgToKafka --> |Wait for all| CloseCircuitBreaker
    
````


---

### SubscriptionComparisonTask

````mermaid
flowchart TB
        Start[Start]

        style Start stroke:red,stroke-width:2px
        
        IsCurrSubInvalid{Is current Subscription invalid?}
        CleanupHealthCache1(Cleanup HealthCache)
        CleanupHealthCache2(Cleanup HealthCache)
        CleanupHealthCache3(Cleanup HealthCache)
        CleanupHealthCache4(Cleanup HealthCache)
        CleanupHealthCache5(Cleanup HealthCache)
        ShouldCBMsgBeHandledByThisPod2{Should CB-Msg be\nhandled by this pod?}
        HowDeliveryTypeChanged{How did the delivery type changed?}
        IsCBOptOut{Is CB-Opt out configured?}
        DidCallbackUrlChange{Did the callback url change?}
        DidHttpMethodChange{Did the http method change?}

        Start --> IsCurrSubInvalid
        IsCurrSubInvalid --> |Yes| CleanupHealthCache1
        IsCurrSubInvalid --> |No| ShouldCBMsgBeHandledByThisPod2

        ShouldCBMsgBeHandledByThisPod2 --> |Yes| HowDeliveryTypeChanged

        HowDeliveryTypeChanged --> |Callback to SSE| CleanupHealthCache2 --> |Spawn| DeliveryTypeChangeTask
        HowDeliveryTypeChanged --> |SSE to Callback\nSpawn| DeliveryTypeChangeTask
        HowDeliveryTypeChanged --> |Callback to Callback| IsCBOptOut

        IsCBOptOut --> |Yes| CleanupHealthCache3 --> |Spawn| SuccessfulHealthRequestTask
        IsCBOptOut --> |No| DidCallbackUrlChange

        DidCallbackUrlChange --> |Yes| CleanupHealthCache4 --> |Update CB-Msg\n\nSpawn| HeathRequestTask
        DidCallbackUrlChange --> |No| DidHttpMethodChange

        DidHttpMethodChange --> |Yes| CleanupHealthCache5 --> |Spawn| HeathRequestTask
        DidHttpMethodChange --> |No\n\nSpawn| HeathRequestTask


        style CleanupHealthCache1 stroke:blue,stroke-width:2px
        style CleanupHealthCache2 stroke:blue,stroke-width:2px
        style CleanupHealthCache3 stroke:blue,stroke-width:2px
        style CleanupHealthCache4 stroke:blue,stroke-width:2px
        style CleanupHealthCache5 stroke:blue,stroke-width:2px
        style HeathRequestTask stroke:purple,stroke-width:2px
        style DeliveryTypeChangeTask stroke:green,stroke-width:2px
        style SuccessfulHealthRequestTask stroke:yellow,stroke-width:2px

````
---
### Method to determine if pod should assign itself to a CircuitBreaker message based on the callback url
````mermaid
flowchart TB
    isAssigned{Is CB-Msg assigned\nto any pod?}
    isOurCallbackHash{Does pods index hash\nmatch CB-Msg hash?}
    isAssignedToUs{Is CB-Msg assigned\nto this pod?}
    style isAssignedToUs stroke:cyan;stroke-with:2px
    isAssignedPodMissing{Is assigned pod dead?}
    ReturnTrue(Return True)
    ReturnFalse(Return False)

    isAssignedToUs --> |Yes| ReturnTrue
    isAssignedToUs --> |No| isOurCallbackHash

    isOurCallbackHash --> |No| ReturnFalse
    isOurCallbackHash --> |Yes| isAssigned

    isAssigned --> |No| ReturnTrue
    isAssigned --> |Yes| isAssignedPodMissing

    isAssignedPodMissing --> |No| ReturnFalse
    isAssignedPodMissing --> |Yes| ReturnTrue

````
---
### Method to cleanup the health cache
````mermaid

flowchart TB
    Start 
    style Start stroke:blue,stroke-width:2px
    
    RemoveSubIdFromHealthCache(Remove SubscriptionId from HealthCache entry for callback url + http method)
    DoOtherEntryExist(Are subscription ids left for callback url + http method?)
    StopHealthRequestTask(Stop HealthRequest Task for callback url + http method)

    Start --> RemoveSubIdFromHealthCache --> DoOtherEntryExist --> |Yes| StopHealthRequestTask
    
    
````
---
### DeliveryTypeChangeTask
````mermaid
flowchart TD
    Start
    style Start stroke:green,stroke-width:2px
    SetCBToREPUBLISHING(Set CB-Msg to REPUBLISHING)
    CloseCircuitBreaker(Close CircuitBreaker)
    FindByStatusInPlusCallbackUrlNotFoundExceptionAsc(Find StatusMessages in\nWAITING\nor\nFAILED with CallbackUrlException\n for CB-Msgs)
    FindByStatusInAndDeliveryTypeAndSubscriptionIdsAsc(Find StatusMessages with delivery type SSE\n for CB-Msgs)
    PickMessagesFromKafka(Pick SubscriptionEventMessages from Kafka)
    SendFAILEDStatusMsgToKafka(Send FAILED StatusMessage to Kafka)
    SendSubEventMsgToKafka(Send SubscriptionEventMessage with PROCESSED to Kafka)
    WasDeliveryTypeToCallback{Was delivery type\nchanged from\nSSE to callback?}


    Start --> SetCBToREPUBLISHING
    SetCBToREPUBLISHING --> WasDeliveryTypeToCallback
    WasDeliveryTypeToCallback --> |Yes| FindByStatusInAndDeliveryTypeAndSubscriptionIdsAsc
    WasDeliveryTypeToCallback --> |No| FindByStatusInPlusCallbackUrlNotFoundExceptionAsc

    FindByStatusInAndDeliveryTypeAndSubscriptionIdsAsc --> PickMessagesFromKafka
    FindByStatusInPlusCallbackUrlNotFoundExceptionAsc --> PickMessagesFromKafka
    PickMessagesFromKafka --> |Successful| SendSubEventMsgToKafka
    PickMessagesFromKafka --> |Unsuccessful| SendFAILEDStatusMsgToKafka

    SendSubEventMsgToKafka --> |Wait for all| CloseCircuitBreaker
    SendFAILEDStatusMsgToKafka --> |Wait for all| CloseCircuitBreaker
````
---
### SuccessfulHealthRequestTask
````mermaid
flowchart TD
    HasHealthCheckCacheEntry{Does health check cache entry exists for callback url + http method?}
    CloseThread(Flag Thread for callback url + http method as closed)
    CloseThread2(Flag Thread for callback url + http method as closed)
    RemoveAllSubIds(Remove all subscription ids)
    IncrementRepublishCount(Increment republish count)
    
    HasHealthCheckCacheEntry --> |No| CloseThread
    HasHealthCheckCacheEntry --> |Yes| RemoveAllSubIds
    RemoveAllSubIds --> CloseThread2

    CloseThread2 --> IncrementRepublishCount


    SetCBToREPUBLISHING(Set CB-Msgs to REPUBLISHING)
    CloseCircuitBreaker(Close CircuitBreakers)
    FindByStatusInPlusCallbackUrlNotFoundExceptionAsc(Find StatusMessages in\nWAITING\nor\nFAILED with CallbackUrlException\n for CB-Msgs)
    PickMessagesFromKafka(Pick SubscriptionEventMessages from Kafka)
    SendFAILEDStatusMsgToKafka(Send FAILED StatusMessage to Kafka)
    SendSubEventMsgToKafka(Send SubscriptionEventMessage with PROCESSED to Kafka)


    IncrementRepublishCount --> SetCBToREPUBLISHING
    SetCBToREPUBLISHING --> FindByStatusInPlusCallbackUrlNotFoundExceptionAsc
    FindByStatusInPlusCallbackUrlNotFoundExceptionAsc --> PickMessagesFromKafka
    PickMessagesFromKafka --> |Successful| SendSubEventMsgToKafka
    PickMessagesFromKafka --> |Unsuccessful| SendFAILEDStatusMsgToKafka

    SendSubEventMsgToKafka --> |Wait for all| CloseCircuitBreaker
    SendFAILEDStatusMsgToKafka --> |Wait for all| CloseCircuitBreaker

    

````
---
### HeathRequestTask
````mermaid
flowchart TD
    Start
    style Start stroke:purple,stroke-width:2px
    FlagOpen(Flag Thread for callback url + http method as open)
    ExecuteHttpRequest(Execute GET or HEAD request to customers endpoint)
    UpdateHealthCheckEntry(Update last health check information\nfor callback url + http method\nin HealthCheckCache\nand CircuitBreakerCache)
    WasRequestSuccessful{Was request successful?}
    
    Start --> FlagOpen
    FlagOpen --> ExecuteHttpRequest
    ExecuteHttpRequest --> UpdateHealthCheckEntry
    UpdateHealthCheckEntry --> WasRequestSuccessful
    WasRequestSuccessful --> |Yes\n\nSpawn| SuccessfulHealthRequestTask 
    WasRequestSuccessful --> |No\n\nSpawn| HealthRequestTask


    style SuccessfulHealthRequestTask stroke:yellow,stroke-width:2px
    style HealthRequestTask stroke:purple,stroke-width:2px

````



