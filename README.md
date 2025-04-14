# th2-conn-dirty-fix (1.5.0)

This microservice allows sending and receiving messages via FIX protocol

## Configuration

+ *sessions* - list of session settings
+ *maxBatchSize* - max size of outgoing message batch (`1000` by default)
+ *maxFlushTime* - max message batch flush time (`1000` by default)
+ *publishSentEvents* - enables/disables publish of "message sent" events (`true` by default)
+ *publishConnectEvents* - enables/disables publish of "connect/disconnect" events (`true` by default)

## Session settings

+ *sessionGroup* - session group for incoming/outgoing th2 messages (equal to session alias by default)
+ *sessionAlias* - session alias for incoming/outgoing th2 messages
+ *handler* - handler settings
+ *mangler* - mangler settings

## Handler settings

+ *host* - service host
+ *port* - service port
+ *security* - connection security settings
+ *beginString* - defines the start of a new message and the protocol version
+ *heartBtInt* - message waiting interval
+ *senderCompID* - ID of the sender of the message
+ *targetCompID* - ID of the message recipient
+ *defaultApplVerID* - specifies the service pack release being applied, by default, to message at the session level
+ *senderSubID* - assigned value used to identify specific message originator (desk, trader, etc.)
+ *encryptMethod* - encryption method
+ *username* - user name
+ *password* - user password. FIX client uses the Password(554) tag for unencrypted mode and the EncryptedPassword(1402) tag for encrypted. The encryption is enabled via *passwordEncryptKeyFilePath* option. It is a good practice to pass this variable as kubernetes secret. 
+ *newPassword* - user new password. FIX client uses the NewPassword(925) tag for unencrypted mode and the NewEncryptedPassword(1404) tag for encrypted. The encryption is enabled via *passwordEncryptKeyFilePath* option. It is a good practice to pass this variable as kubernetes secret.
+ *previousPasswords* - comma-separated list of passwords used for this user before.
+ *passwordEncryptKeyFilePath* - path to key file for encrypting. FIX client encrypts the *password* value via `RSA` algorithm using specified file if this option is specified.
+ *passwordEncryptKeyFileType* - type of key file content. Supported values: `[PEM_PUBLIC_KEY]`. Default value is `PEM_PUBLIC_KEY`
+ *passwordKeyEncryptAlgorithm* - encrypt algorithm for reading key from file specified in the *passwordEncryptKeyFilePath*. See the KeyFactory section in the [Java Cryptography Architecture Standard Algorithm Name](https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#KeyFactory) for information about standard algorithm names. Default value is `RSA`
+ *passwordEncryptAlgorithm* - encrypt algorithm for encrypting passwords specified in the *password* and *newPassword*. See the Cipher section in the [Java Cryptography Architecture Standard Algorithm Name](https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#Cipher) for information about standard transformation names. Default value is `RSA`
+ *testRequestDelay* - interval for test request
+ *reconnectDelay* - interval for reconnect
+ *disconnectRequestDelay* - the interval for the shutdown request
+ *resetSeqNumFlag* - resetting sequence number in initial Logon message (when conn started)
+ *resetOnLogon* - resetting the sequence number in Logon in other cases (e.g. disconnect)
+ *loadSequencesFromCradle* - defines if sequences will be loaded from cradle to use them in logon message.
+ *loadMissedMessagesFromCradle* - defines how retransmission will be handled. If true, then requested through `ResendRequest` messages (or messages requested on Logon with `NextExpectedSeqNum`) will be loaded from cradle.
+ *sessionStartTime* - UTC time when session starts. (`nullable`)
+ *sessionEndTime* - UTC time when session ends. required if startSessionTime is filled.
+ *sendingDateTimeFormat* - `SendingTime` field format for outgoing messages. (`nullable`, `default format` in this case is `"yyyyMMdd-HH:mm:ss.SSSSSSSSS"`) 
+ *useNextExpectedSeqNum* - session management based on next expected sequence number. (`false` by default)
+ *saveAdminMessages* - defines if admin messages will be saved to internal outgoing buffer. (`false` by default)
+ *resetStateOnServerReset* - whether to reset the server sequence after receiving logout with text `Next Expected MSN too high, MSN to be sent is x but received y`.
+ *logoutOnIncorrectServerSequence* - whether to logout session when server send message with sequence number less than expected. If `false` then internal conn sequence will be reset to sequence number from server message.
+ *connectionTimeoutOnSend* - timeout in milliseconds for sending message from queue thread
  (please read about [acknowledgment timeout](https://www.rabbitmq.com/consumers.html#acknowledgement-timeout) to understand the problem).
  _Default, 30000 mls._ Each failed sending attempt decreases the timeout in half (but not less than _minConnectionTimeoutOnSend_).
  The timeout is reset to the original value after a successful sending attempt.
  If connection is not established within the specified timeout an error will be reported.
+ *minConnectionTimeoutOnSend* - minimum value for the sending message timeout in milliseconds. _Default value is 1000 mls._

### Security settings

+ *ssl* - enables SSL on connection (`false` by default)
+ *sni* - enables SNI support (`false` by default)
+ *certFile* - path to server certificate (`null` by default)
+ *acceptAllCerts* - accept all server certificates (`false` by default, takes precedence over `certFile`)

## Mangler settings

Mangler is configured by specifying a list of transformations which it will try to apply to outgoing messages.   
Each transformation has a list of conditions which message must meet for transformation actions to be applied.

Condition is basically a field value check:

```yaml
tag: 35
matches: (8|D)
```

Where `tag` is a field tag to match and `matches` is a regex pattern for the field value.

Conditions are specified in `when` block of transformation definition:

```yaml
when:
  - tag: 35
    matches: (8|D)
  - tag: 49
    matches: SENDER(.*)
```

Actions describe modifications which will be applied to a message. There are 4 types of actions:

* set - sets value of an existing field to the specified value:

  ```yaml
  set:
    tag: 1
    value: new account
  ```

* add - adds new field before or after an existing field:

  ```yaml
  add:
    tag: 15
    value: USD
  after: # or before
    tag: 58
    matches: (.*)
  ```

* move - moves an existing field before or after another field:

  ```yaml
  move:
    tag: 49
    matches: (.*)
  after: # or before
    tag: 56
    matches: (.*)
  ```

* replace - replaces an existing field with another field:

  ```yaml
  replace:
    tag: 64
    matches: (.*)
  with:
    tag: 63
    value: 1
  ```

* remove - removes an existing field:

  ```yaml
  remove:
    tag: 110
    matches: (.*)
  ```

Actions are specified in `then` block of transformation definition:

```yaml
then:
  - set:
      tag: 1
      value: new account
  - remove:
      tag: 110
      matches: (.*)
```

Transformation can also automatically recalculate length and checksum if any actions were applied.  
This is controlled by `update-length` and `update-checksum` (both `true` by default) transformation options.

Full config will be divided into groups of transforms united by rules, each rule will have `name` as key and list of transforms. Only one rule can be triggered, after conditions tests triggered rules will be united into specific list and
only one random rule (group of transforms) will be chosen.

```yaml
rules:
  - name: rule-1
    transform: [ ... ]
  - name: rule-99
    transform: [ ... ]
```

Complete mangler configuration would look something like this:

```yaml
mangler:
  rules:
    - name: rule-1
      transform:
        - when:
            - tag: 8
              matches: FIXT.1.1
            - tag: 35
              matches: D
          then:
            - set:
                tag: 1
                value: new account
            - add:
                tag: 15
                value: USD
              after:
                tag: 58
                matches: (.*)
          update-length: false
        - when:
            - tag: 8
              matches: FIXT.1.1
            - tag: 35
              matches: 8
          then:
            - replace:
                tag: 64
                matches: (.*)
              with:
                tag: 63
                value: 1
            - remove:
                tag: 110
                matches: (.*)
          update-checksum: false
```

### Ways to use mangler:
1) Rule described in CR: rule will be applied if message passes `when` condition 
2) Rule is described in CR and message contains property `rule-name`: rule with name from `rule-name` property value will be applied to message despite of `when` block.
<br />example - rule with name `5` that described in CR will be applied:
<br />```... .setMetadata(MessageMetadata.newBuilder(builder.getMetadata()).putProperties("rule-name", "5").build()) ...```
3) Rule is described in message `rule-actions` property: rule will be applied to message as described in property `rule-actions`
<br />example - value of tag 44 will be changed to `åÅæÆøØ`
<br />```... .setMetadata(MessageMetadata.newBuilder(builder.getMetadata()).putProperties("rule-actions", "[{\"set\":{\"tag\": 44,\"value\": \"åÅæÆøØ\"}}]").build()) ...```

## Broken Strategies configuration
This component allows to configure chaos rules for protocol level testing.
```yaml
brokenConnConfiguration:
  schedulerType: CONSECUTIVE
  rules:
    - ruleType: STRATEGY_NAME
      duration: PT5S
      cleanUpDuration: PT5S
      weight: null
      strategyRelatedConfiguration:

```

### Scheduler types
*schedulerType* allows to configure how rule to be executed will be selected:
- CONSECUTIVE - rules will be executed one by one in order they are defined in *rules* configuration.
- WEIGHT - rules will be randomly selected from *rules* configuration. Probability of the rule to be selected from rules list is depends on *weight* option: the more weight rule has the more likely to be selected for execution.

### General rule configurations
- *ruleType* - strategy name defines how connector will behave when this rule is active.
- *duration* - how much time rule will be active and affect connector behavior.
- *cleanUpDuration* - how much time connector is needed to go back to normal session after rule strategy deactivation.
- *weight* - optional parameter if *schedulerType* is set to WEIGHT, defines probability of this rule to be selected by strategy scheduler.

### Rule Type: LOGON_FROM_ANOTHER_CONNECTION
```yaml
ruleType: "LOGON_FROM_ANOTHER_CONNECTION"
duration: "PT5S"
cleanUpDuration: "PT2S"
```
When this rule is activated another tcp socket with gateway host:port opened and logon sent through this new connection while previous session is still active.
There is no rule specific configurations.

### Rule Type: CLIENT_OUTAGE
```yaml
ruleType: "CLIENT_OUTAGE"
duration: "PT32S"
cleanUpDuration: "PT5S"
```
This rule emulates client outage behaviour: client is not responding to any gateway messages including heartbeats and test requests.
There is no rule specific configurations.

### Rule Type: PARTIAL_CLIENT_OUTAGE
```yaml
ruleType: "CLIENT_OUTAGE"
duration: "PT32S"
cleanUpDuration: "PT5S"
```

This rule emulates partial client outage: client is not responding to any gateway messages except TestRequest message.
There is no rule specific configurations.

### Rule Type: DISCONNECT_WITH_RECONNECT
```yaml
ruleType: "DISCONNECT_WITH_RECONNECT"
duration: "PT1S"
cleanUpDuration: "PT2S"
gracefulDisconnect: true
allowMessagesBeforeLogonReply: false
```

This rule triggers gracefull / ungracefull session disconnect and reconnects.

Rule Specific Configurations:
- **gracefulDisconnect** - defines whenever client should close session gracefully, i.e by sending logout message and waiting for reply or ungracefully by just closing tcp session.
- **allowMessagesBeforeLogonReply** - if set to true allows client to send outgoing business messages before session was fully established, i.e logon is sent to gateway and gateway answered with logon response back.

### Rule Type: IGNORE_INCOMMING_MESSAGES
```yaml
ruleType: "IGNORE_INCOMING_MESSAGES"
duration: "PT2S"
cleanUpDuration: "PT2S"
missIncomingMessagesConfiguration:
    count: 4
```

When this rule is active, gateway gap behavior is emulated: N incoming gateway messages are ignored and resend request is sent when gap is detected.

Rule specific configurations:
- **missIncomingMessagesConfiguration.count** - defines how many gateway messages should be ignored

### Rule Type: TRANSFORM_LOGON
```yaml
ruleType: TRANSFORM_LOGON
duration: "PT30S"
cleanUpDuration: "PT5S"
transformMessageConfiguration:
  transformations:
    - messageType: A
      comment: "invalid_password"
      actions:
        - set:
            tag: 1402
            value: "invalid_password" 
    - messageType: 'A'
      comment: "invalid_key_and_method"
      newPassword: "new_password"
      passwordKeyEncryptionAlgorithm: "RSA"
      passwordEncryptionAlgorithm: "RSA/OAEP"
      encryptKey: public_key
```

When this rule is active corrupted logon messages are sent to gateway.

Rule specific configurations:
- **transformations** - list of transformations for logon messages. They are applied one by one for different logon messages.
- **messageType** - message type to corrupt, for TRANSFORM_LOGON strategy should be allway equal to `A`
- **comment** - transformation comment is put into `transformationComment` property of th2 message metadata related to Logon. Can be used in later scenarios execution analysis.
- **actions** - list of tag related actions like set, remove, replace to be executed on message. Format is the same as for **mangler** configuration.
- **newPassword** - password to be used in logon message.
- **passwordEncryptionAlgorithm** - algorithm that will be used either **newPassword** if set or password defined in connect configuration.
- **encryptKey** - public key which can be used by **passwordEncryptionAlgorithm** option.
- **passwordKeyEncryptionAlgorithm** - **encryptKey** encryption method.

### Rule Type: CREATE_OUTGOING_GAP
```yaml
ruleType: "CREATE_OUTGOING_GAP"
duration: "PT2S"
cleanUpDuration: "PT2S"
allowMessagesBeforeRetransmissionFinishes: false
recoveryConfig:
  outOfOrder: false
  sequenceResetForAdmin: true
missOutgoingMessagesConfiguration:
  count: 4
```
This strategy emulates client gap behaviour: client skips and doesn't send configured amount of outgoing messages, gateway should detect a gap and send resend request to client

Rule specific configurations:
- **allowMessagesBeforeRetransmissionFinishes** - when set to true, new business messages can be sent in between recovery messages.
  **recoveryConfig.outOfOrder** - when set to true, recovery messages is sent to gateway out of order of their sequence numbers.
  **recoveryConfig.sequenceResetForAdmin** - when set to false, Admin messages like logon, logout, etc that are part of requested gap are sent as is instead of SequenceReset with GapFill flag.
- **missOutgoingMessagesConfiguration.count** - defines how many outgoing messages client should skip and shouldn't send to gateway, to create gap.

### Rule Type: RESEND_REQUEST
```yaml
ruleType: "RESEND_REQUEST"
duration: "PT2S"
cleanUpDuration: "PT2S"
resendRequestConfiguration:
  messageCount: 5
  range: true
  single: false
  untilLast: false
```

When this strategy is activated - `ResendRequest` is sent to gateway with configured parameters.

Rule specific configurations:
- **messageCount** - how many messages should be requested in resend request
- **range** - if true, range of messages will be requested with specific values for `begin` and `end` intervals.
- **single** - if true, single message will be requested and begin and end interval will be equal to each other.
- **untilLast** - if true, range of messages will requested without defined end interval, all messages that gateway has to recover past begin interval number will be requested.

### Rule Type: SEQUENCE_RESET
```yaml
ruleType: "SEQUENCE_RESET"
duration: PT1S
cleanUpDuration: PT2S
changeSequenceConfiguration:
  messageCount: 5
  changeIncomingSequence: true
  sendLogoutAfterReset: true
  changeUp: true
  gapFill: true
```

This strategy changes client's internal `next expected client sequence number` or `next expected gateway sequence number` value and reconnects the session.

Rule specific configuration parameters:
- **messageCount** - by which amount `next expected sequence number` will be changed.
- **changeIncommingSequenceNumber** - if true `next expected sequence number` from gateway will be changed up / down for **messageCount** amount else sequence number of the next message that client will sent will be changed.
- **changeUp** - if true, `next expected sequence number` will be increased by **messageCount** amount, if false `next expected sequence number` will be descreased by **messageCount** amount.
- **sendLogoutAfterReset** - if true, disconnects session after `next expected sequence number` change, otherwise disconnects session before `next expected sequence number` change.
- **gapFill** -  if true and recovery is issued by gateway after `next expected sequence number` update then `GapFillFlag` tag will be set in `SequenceReset` message used for recovery, if false `GapFillFlag` will not be used.

### Rule Type: SEND_SEQUENCE_RESET
```yaml
ruleType: "SEND_SEQUENCE_RESET"
duration: PT1S
cleanUpDuration: PT2S
sendSequenceResetConfiguration:
  changeUp: true
```

When this strategy is activated `SequenceReset` message is sent to gateway with configured parameters.
Rule specific configuration parameters:
- **changeUp** - if true, `SequenceReset` message will set `NewSeqNo` tag to sequence number which is more than current session sequence number, otherwise `NewSeqNo` will be set to sequence number less than current session sequence number

### Rule Type: FAKE_RETRANSMISSION
```yaml
ruleType: "FAKE_RETRANSMISSION"
duration: PT1S
cleanUpDuration: PT2S
```
This strategy adds `PossDupFlag=Y` to normal business messages which are not part of the retransmission.

### Rule Type: POSS_DUP_SESSION_MESSAGES
```yaml
ruleType: "POSS_DUP_SESSION_MESSAGES"
duration: PT1S
cleanUpDuration: PT2S
```

When this strategy activated all session messages are sent to gateway with `PossDupFlag` set to `Y`: `Logon`, `Logout`, `TestRequest`, `Heartbeat`, `SequenceReset`.

### Rule Type: CORRUPT_MESSAGE_STRUCTURE
```yaml
ruleType: "CORRUPT_MESSAGE_STRUCTURE"
duration: PT1S
cleanUpDuration: PT2S
corruptMessageConfiguration:
  headerTags: [...]
  trailerTags: [...]
  moveHeaderConfiguration:
    position: AFTER_BODY
```

```yaml
ruleType: "CORRUPT_MESSAGE_STRUCTURE"
duration: PT1S
cleanUpDuration: PT2S
corruptMessageConfiguration:
  headerTags: [...]
  trailerTags: [...]
  moveTrailerConfiguration:
    position: BEFORE_HEADER
```

When this strategy is executed, message structure corruptions are applied to outgoing messages: correct message structure `header` - `body` - `trailer` is changed to other permutations of those 3 components.

Rule specific configuration:
- **headerTags** - fix tag numbers that belongs to header
- **trailerTags** - fix tag numbers that belongs to trailer
- **moveHeaderConfiguration.position** - defines the position where message `header` should be moved from the start of the message. Possible values: [INSIDE_BODY, AFTER_BODY, INSIDE_TRAILER, AFTER_TRAILER]
- **moveTrailerConfiguration.position** - defines the position where message `trailer` should be moved from the end of the message. Possible values: [BEFORE_HEADER, INSIDE_HEADER, AFTER_HEADER, INSIDE_BODY]

### Rule Type: ADJUST SENDING TIME
```yaml
ruleType: "ADJUST_SENDING_TIME"
duration: PT1S
cleanUpDuration: PT2S
adjustSendingTimeConfiguration:
  adjustDuration: "PT180S"
  substract: false
```

When this strategy is activated `SendingTime` field is changed up or down depending on configuration.

Rule specific configuration:
- **adjustDuration** - by how much time `SendingTime` filed should be adjusted in the future or in the past. `Duration` java class is used to configure this setting.
- **substract** - if true, `SendingTime` will be changed to the time in the past by `adjustDuration`, if false `SendingTime` will be changed to the time in the future by `adjustDuration`.

### Rule Type: TRIGGER_LOGOUT
```yaml
ruleType: "TRIGGER_LOGOUT"
duration: PT1S
cleanUpDuration: PT2S
sendResendRequestOnLogoutReply: true
```

When this strategy is activated `Logout` from gateway is caused by sending message with `SequenceNumber` less then gateway expects.

Rule specific configuration:
**sendResendRequestOnLogoutReply** - if set to true, `ResendRequest` will be send to gateway before sending logout response to gateway `Logout`.

### Rule Type: POSS_RESEND
```yaml
ruleType: "POSS_RESEND"
duration: PT1S
cleanUpDuration: PT2S
```

When this strategy is activated all outgoing business messages will be sent twice, first time with `PossResendFlag=N` and second time with `PossResendFlag=Y`.

### Rule Type: DUPLICATE REQUEST
```yaml
ruleType: "POSS_RESEND"
duration: PT1S
cleanUpDuration: PT2S
duplicateRequestConfiguration:
  allowedMessageTypes: ['D']
```

When this strategy is active all allowed outgoing messages sent twice to gateway.

Rule specific configuration:
- **allowedMessageTypes** - list of message types that may be duplicated.

### Rule Type: NEGATIVE_STRUCTURE_TESTING
```yaml
ruleType: "NEGATIVE_STRUCTURE_TESTING"
duration: PT1S
cleanUpDuration: PT2S
negativeStructureConfiguration:
  afterSendTimeoutMs: 150
```

When this strategy is active outgoing business messages are corrupted by different transformation types applied to header / trailer fields:
- Remove tag corruption - removes required tag
- Null tag corruption - sets \0 as tag value
- Empty tag corruption - sets tag equal to empty string
- Duplicate tag corruption - makes tag appear twice in the message
- Out of range tag corruption - sets value out of allowed range for tag
- Negative number tag corruption - sets tag value to negative value for tags with only positive numbers allowed
- Invalid data type tag corruption - sets tag value to value which has incorrect data type: for numeric tags string value, for data tags numeric value, for boolean tags - string value
- Move tag out of order corruption - swaps target tag with tag before
- Tag that is not exists corruption - adds tag that not exists in fix specification
- Tag that not belongs to header corruption - adds tag that is not belongs to header into header
- Tag that not belongs to trailer corruption - adds tag that is not belongs to trailer into trailer

Corruptions are applied one by one for each header and trailer field.

Rule specific configurations:
- **afterSendTimeoutMs** - time in milliseconds to wait after corrupted message was sent to gateway before corrupting next message, if gateway is taking too much time to answer to corrupted message this parameter can be increased.

### Rule Type: NEGATIVE_STRUCTURE_TESTING_SESSION_MESSAGES
```yaml
ruleType: "NEGATIVE_STRUCTURE_TESTING_SESSION_MESSAGES"
duration: PT1S
cleanUpDuration: PT2S
negativeStructureConfiguration:
  afterSendTimeoutMs: 150
```

When this strategy is active outgoing session messages are corrupted with different types of corruptions applied to different header / body / trailer fields:
- Remove tag corruption - removes required tag
- Null tag corruption - sets \0 as tag value
- Empty tag corruption - sets tag equal to empty string
- Duplicate tag corruption - makes tag appear twice in the message
- Out of range tag corruption - sets value out of allowed range for tag
- Negative number tag corruption - sets tag value to negative value for tags with only positive numbers allowed
- Invalid data type tag corruption - sets tag value to value which has incorrect data type: for numeric tags string value, for data tags numeric value, for boolean tags - string value
- Move tag out of order corruption - swaps target tag with tag before
- Tag that is not exists corruption - adds tag that not exists in fix specification
- Tag that not belongs to header corruption - adds tag that is not belongs to header into header
- Tag that not belongs to trailer corruption - adds tag that is not belongs to trailer into trailer

Corruptions are applied one by one for each header field, for required body fields and for all trailer fields.

Rule specific configurations:
- **afterSendTimeoutMs** - time in milliseconds to wait after corrupted message was sent to gateway before corrupting next message, if gateway is taking too much time to answer to corrupted message this parameter can be increased.

### Rule Type: TRANSFORM_MESSAGE_STRATEGY
```yaml
ruleType: "TRANSFORM_MESSAGE_STRATEGY"
duration: PT15S
cleanUpDuration: PT2S
transformMessageConfiguration:
  transformations:
    - actions:
        - set: 49
          value: invalid
    - actions:
        - add: 
            tag: 35
            value: D
          after: 
            tag: 10
            matching: (.*)
```

When this strategy is active a list of transformations sequentially applied to outgoing messages.

Rule specific configuration:
- **transformations** - List of message transformation rules that will be sequentially applied to different outgoing messages.
- **transformations.actions** - Transformation rule declaration in mangler format.

## MQ pins

+ input queue with `subscribe`, `send` and `raw` attributes for outgoing messages
+ output queue with `publish`, `first` (for incoming messages) or `second` (for outgoing messages) and `raw` attributes

## Deployment via infra-mgr

Here's an example of `infra-mgr` config required to deploy this service

```yaml
apiVersion: th2.exactpro.com/v2
kind: Th2Box
metadata:
  name: fix-client
spec:
  imageName: ghcr.io/th2-net/th2-conn-dirty-fix
  imageVersion: 1.0.0
  type: th2-conn
  customConfig:
    useTransport: true
    maxBatchSize: 1000
    maxFlushTime: 1000
    batchByGroup: true
    publishSentEvents: true
    publishConnectEvents: true
    sessions:
      - sessionAlias: client
        security:
          ssl: false
          sni: false
          certFile: ${secret_path:cert_secret}
          acceptAllCerts: false
        host: "<host>"
        port: "<port>"
        maxMessageRate: 100000
        autoReconnect: true
        reconnectDelay: 5000
        handler:
          beginString: FIXT.1.1
          heartBtInt: 30
          senderCompID: client
          targetCompID: FGW
          encryptMethod: 0
          username: username
          password: password
          resetSeqNumFlag: false
          resetOnLogon: false
          testRequestDelay: 60
          reconnectDelay": 5
          disconnectRequestDelay: 5
        mangler:
          rules:
            - name: rule-1
              transform:
                - when:
                    - { tag: 8, matches: FIXT.1.1 }
                    - { tag: 35, matches: D }
                  then:
                    - set: { tag: 1, value: new account }
                    - add: { tag: 15, valueOneOf: ["USD", "EUR"] }
                      after: { tag: 58, matches: (.*) }
                  update-length: false
                - when:
                    - { tag: 8, matches: FIXT.1.1 }
                    - { tag: 35, matches: 8 }
                  then:
                    - replace: { tag: 64, matches: (.*) }
                      with: { tag: 63, value: 1 }
                    - remove: { tag: 110, matches: (.*) }
                  update-checksum: false
  pins:
    mq:
      subscribers:
        - name: to_send 
          attributes:
            - transport-group
            - subscribe
            - send
          filters:
            - metadata:
                - fieldName: direction
                  expectedValue: FIRST
                  operation: EQUAL
      publishers:
        - name: to_mstore
          attributes:
            - transport-group
            - publish
    grpc:
      client:
        - name: to_data_provider
          serviceClass: com.exactpro.th2.dataprovider.grpc.DataProviderService
  extended-settings:
    externalBox:
      enabled: false
    service:
      enabled: false
    resources:
      limits:
        memory: 200Mi
        cpu: 600m
      requests:
        memory: 100Mi
        cpu: 20m
```

# Changelog

## 1.5.0

* Merged changes from [th2-conn-dirty-fix:1.8.0](https://github.com/th2-net/th2-conn-dirty-fix)
* Updated:
  * httpclient5: `5.4.3`

## 1.4.2

* Add property `encode-mode: dirty` for messages that are corrupted with transformation strategies: `TRANSFORM_MESSAGE_STRATEGY`, `INVALID_CHECKSUM` and `TRANSFORM_LOGON`
* Added synchronization for business messages sent via rabbitmq and messages that are sent from handler.

## 1.4.1

* Use keep open gRPC query to recover messages for Resend Request
* Update `com.exactpro.th2.gradle` plugin to `0.1.3`

## 1.4.0

* Provided ability to disable transformation strategies: `TRANSFORM_MESSAGE_STRATEGY`, `INVALID_CHECKSUM`, `FAKE_RETRANSMISSION` for raw message types specified in the `disableForMessageTypes` property

## 1.3.0

* Fixed the problem long recovery in case of mixing recovery message with non-recovery messages
* Migrated to th2 gradle plugin `0.1.1`
* Updated:
  * bom: `4.6.1`
  * common: `5.13.1-dev`
  * common-utils: `2.2.3-dev`
  * conn-dirty-tcp-core: `3.6.0-dev`
  * grpc-lw-data-provider: `2.3.1-dev`
  * httpclient5: `5.3.1`
  * auto-service: `1.1.1`
  * kotlin-logging: `3.0.5`

## 1.2.1

* Property `th2.broken.strategy` is added to metadata to each message when a strategy is active

## 1.2.0
* Added support for th2 transport protocol
* Header tags are forced to update by conn
* Merged changes from [th2-conn-dirty-fix:1.5.1](https://github.com/th2-net/th2-conn-dirty-fix)

## 1.1.0
* state reset option on server update.

## 1.0.2
* dev releases
* apply changes from version-0

## 1.0.1
* Add bookId to lw data provider query

## 1.0.0
* Bump `conn-dirty-tcp-core` to `3.0.0` for books and pages support

## 0.3.0
* Ability to recover messages from cradle.

## 0.2.0
* optional state reset on silent server reset.

## 0.1.1
* correct sequence numbers increments.
* update conn-dirty-tcp-core to `2.3.0`

## 0.1.0
* correct handling of sequence reset with `endSeqNo = 0`
* Skip messages mangling on error in `demo-fix-mangler` with error event instead of throwing exception.
* allow unconditional rule application

## 0.0.10
* disable reconnect when session is in not-active state.

## 0.0.9
* correct heartbeat and test request handling

## 0.0.8

* th2-common upgrade to `3.44.1`
* th2-bom upgrade to `4.2.0`

## 0.0.7

* wait for acceptor logout response on close
* load sequences from lwdp

## 0.0.6

* wait for logout to be sent

## 0.0.5

* copy messages before putting them into cache

## 0.0.4

* Session management based on NextExpectedSeqNum field.
* Recovery handling
    * outgoing messages are now saved
    * if message wasn't saved sequence reset message with gap fill mode flag is sent.
* Session start and Session end configuration to handle sequence reset by exchange schedule.

## 0.0.3

* Added new password option into settings
* Provided ability to specify encrypt algorithm for reading key from file and encrypting password and new password fields

## 0.0.2

* Supported the password encryption via `RSA` algorithm.