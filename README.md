# th2-conn-dirty-fix (1.3.0)

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

## MQ pins

+ input queue with `subscribe`, `send` and `raw` attributes for outgoing messages
+ output queue with `publish`, `first` (for incoming messages) or `second` (for outgoing messages) and `raw` attributes

## Deployment via infra-mgr

Here's an example of `infra-mgr` config required to deploy this service

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: fix-client
spec:
  image-name: ghcr.io/th2-net/th2-conn-dirty-fix
  image-version: 1.0.0
  type: th2-conn
  custom-config:
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
    - name: to_data_provider
      connection-type: grpc-client
      service-class: com.exactpro.th2.dataprovider.grpc.DataProviderService
    - name: to_send
      connection-type: mq
      attributes:
        - subscribe
        - send
        - raw
      settings:
        storageOnDemand: false
        queueLength: 1000
    - name: incoming_messages
      connection-type: mq
      attributes:
        - publish
        - store
        - raw
      filters:
        - metadata:
            - field-name: direction
              expected-value: FIRST
              operation: EQUAL
    - name: outgoing_messages
      connection-type: mq
      attributes:
        - publish
        - store
        - raw
      filters:
        - metadata:
            - field-name: direction
              expected-value: SECOND
              operation: EQUAL
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

## 1.3.0

* Provided ability to disable transformation strategies: `TRANSFORM_MESSAGE_STRATEGY`, `INVALID_CHECKSUM`, `FAKE_RETRANSMISSION` for raw message types specified in the `disableForMessageTypes` property

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
