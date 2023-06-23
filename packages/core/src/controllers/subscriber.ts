import {EventEmitter} from "events";
import {HEARTBEAT_EVENTS} from "@walletconnect/heartbeat";
import {ErrorResponse, RequestArguments} from "@walletconnect/jsonrpc-types";
import {generateChildLogger, getLoggerContext, Logger} from "@walletconnect/logger";
import {RelayJsonRpc} from "@walletconnect/relay-api";
import {Watch} from "@walletconnect/time";
import {
    IRelayer,
    ISubscriber,
    RelayerTypes,
    SubscriberEvents,
    SubscriberTypes,
} from "@walletconnect/types";
import {
    getSdkError,
    getInternalError,
    getRelayProtocolApi,
    getRelayProtocolName,
    createExpiringPromise,
    hashMessage,
    isValidArray,
} from "@walletconnect/utils";
import {
    CORE_STORAGE_PREFIX,
    SUBSCRIBER_CONTEXT,
    SUBSCRIBER_EVENTS,
    SUBSCRIBER_STORAGE_VERSION,
    PENDING_SUB_RESOLUTION_TIMEOUT,
    RELAYER_EVENTS,
} from "../constants";
import {SubscriberTopicMap} from "./topicmap";

export class Subscriber extends ISubscriber {
    public subscriptions = new Map<string, SubscriberTypes.Active>();
    public topicMap = new SubscriberTopicMap();
    public events = new EventEmitter();
    public name = SUBSCRIBER_CONTEXT;
    public version = SUBSCRIBER_STORAGE_VERSION;
    public pending = new Map<string, SubscriberTypes.Params>();

    private cached: SubscriberTypes.Active[] = [];
    private initialized = false;
    private pendingSubscriptionWatchLabel = "pending_sub_watch_label";
    private pollingInterval = 20;
    private storagePrefix = CORE_STORAGE_PREFIX;
    private subscribeTimeout = 10_000;
    private restartInProgress = false;
    private clientId: string;
    private batchSubscribeTopicsLimit = 500;

    constructor(public relayer: IRelayer, public logger: Logger) {
        super(relayer, logger);
        this.relayer = relayer;
        this.logger = generateChildLogger(logger, this.name);
        this.clientId = ""; // assigned in init
    }

    public init: ISubscriber["init"] = async () => {
        try {
            if (!this.initialized) {
                this.logger.trace(`Initialized`);
                await this.restart();
                this.registerEventListeners();
                this.onEnable();
                this.clientId = await this.relayer.core.crypto.getClientId();
            }
        } catch (e) {
            console.log('subscriber.init error', e)
        }
    };

    get context() {
        return getLoggerContext(this.logger);
    }

    get storageKey(): string {
        return this.storagePrefix + this.version + "//" + this.name;
    }

    get length() {
        try {
            return this.subscriptions.size;
        } catch (e) {
            console.log('subscriber.length error', e)
        }
    }

    get ids() {
        try {
            return Array.from(this.subscriptions.keys());
        } catch (e) {
            console.log('subscriber.ids error', e)
        }
    }

    get values() {
        try {
            return Array.from(this.subscriptions.values());
        } catch (e) {
            console.log('subscriber.values error', e)
        }
    }

    get topics() {
        try {
            return this.topicMap.topics;
        } catch (e) {
            console.log('subscriber.topics error', e)
        }
    }

    public subscribe: ISubscriber["subscribe"] = async (topic, opts) => {
        try {
            await this.restartToComplete();
            this.isInitialized();
            this.logger.debug(`Subscribing Topic`);
            this.logger.trace({type: "method", method: "subscribe", params: {topic, opts}});
        } catch (e) {
            console.log('subscriber.subscribe error1', e)
        }
        try {
            const relay = getRelayProtocolName(opts);
            const params = {topic, relay};
            this.pending.set(topic, params);
            const id = await this.rpcSubscribe(topic, relay);
            this.onSubscribe(id, params);
            this.logger.debug(`Successfully Subscribed Topic`);
            this.logger.trace({type: "method", method: "subscribe", params: {topic, opts}});
            return id;
        } catch (e) {
            console.log('subscriber.subscribe error2', e)
            this.logger.debug(`Failed to Subscribe Topic`);
            this.logger.error(e as any);
            throw e;
        }
    };

    public unsubscribe: ISubscriber["unsubscribe"] = async (topic, opts) => {
        try {
            await this.restartToComplete();
            this.isInitialized();
            if (typeof opts?.id !== "undefined") {
                await this.unsubscribeById(topic, opts.id, opts);
            } else {
                await this.unsubscribeByTopic(topic, opts);
            }
        } catch (e) {
            console.log('subscriber.unsubscribe error', e)
        }
    };

    public isSubscribed: ISubscriber["isSubscribed"] = async (topic: string) => {
        // topic subscription is already resolved
        try {
            if (this.topics.includes(topic)) return true;
            // wait for the subscription to resolve
            const exists = await new Promise<boolean>((resolve, reject) => {
                const watch = new Watch();
                watch.start(this.pendingSubscriptionWatchLabel);
                const interval = setInterval(() => {
                    if (!this.pending.has(topic) && this.topics.includes(topic)) {
                        clearInterval(interval);
                        watch.stop(this.pendingSubscriptionWatchLabel);
                        resolve(true);
                    }
                    if (watch.elapsed(this.pendingSubscriptionWatchLabel) >= PENDING_SUB_RESOLUTION_TIMEOUT) {
                        clearInterval(interval);
                        watch.stop(this.pendingSubscriptionWatchLabel);
                        reject(new Error("Subscription resolution timeout"));
                    }
                }, this.pollingInterval);
            }).catch(() => false);
            return exists;
        } catch (e) {
            console.log('subscriber.isSubscribed error', e)
        }
    };

    public on: ISubscriber["on"] = (event, listener) => {
        try {
            this.events.on(event, listener);
        } catch (e) {
            console.log('subscriber.one error', e)
        }
    };

    public once: ISubscriber["once"] = (event, listener) => {
        try {
            this.events.once(event, listener);
        } catch (e) {
            console.log('subscriber.once error', e)
        }
    };

    public off: ISubscriber["off"] = (event, listener) => {
        try {
            this.events.off(event, listener);
        } catch (e) {
            console.log('subscriber.off error', e)
        }
    };

    public removeListener: ISubscriber["removeListener"] = (event, listener) => {
        try {
            this.events.removeListener(event, listener);
        } catch (e) {
            console.log('subscriber.removeListiner error', e)
        }
    };

    // ---------- Private ----------------------------------------------- //

    private hasSubscription(id: string, topic: string) {
        let result = false;
        try {
            const subscription = this.getSubscription(id);
            result = subscription.topic === topic;
        } catch (e) {
            // ignore error
        }
        return result;
    }

    private onEnable() {
        this.cached = [];
        this.initialized = true;
    }

    private onDisable() {
        try {
            this.cached = this.values;
            this.subscriptions.clear();
            this.topicMap.clear();
        } catch (e) {
            console.log('subscriber.onDisable error', e)
        }
    }

    private async unsubscribeByTopic(topic: string, opts?: RelayerTypes.UnsubscribeOptions) {
        try {
            const ids = this.topicMap.get(topic);
            await Promise.all(ids.map(async (id) => await this.unsubscribeById(topic, id, opts)));
        } catch (e) {
            console.log('subscriber.unsubscribeByTopic error', e)
        }
    }

    private async unsubscribeById(topic: string, id: string, opts?: RelayerTypes.UnsubscribeOptions) {
        try {
            this.logger.debug(`Unsubscribing Topic`);
            this.logger.trace({type: "method", method: "unsubscribe", params: {topic, id, opts}});
            try {
                const relay = getRelayProtocolName(opts);
                await this.rpcUnsubscribe(topic, id, relay);
                const reason = getSdkError("USER_DISCONNECTED", `${this.name}, ${topic}`);
                await this.onUnsubscribe(topic, id, reason);
                this.logger.debug(`Successfully Unsubscribed Topic`);
                this.logger.trace({type: "method", method: "unsubscribe", params: {topic, id, opts}});
            } catch (e) {
                this.logger.debug(`Failed to Unsubscribe Topic`);
                this.logger.error(e as any);
                throw e;
            }
        } catch (e) {
            console.log('subscriber.unsubscribeById error', e)
        }
    }

    private async rpcSubscribe(topic: string, relay: RelayerTypes.ProtocolOptions) {
        try {
            const api = getRelayProtocolApi(relay.protocol);
            const request: RequestArguments<RelayJsonRpc.SubscribeParams> = {
                method: api.subscribe,
                params: {
                    topic,
                },
            };
            this.logger.debug(`Outgoing Relay Payload`);
            this.logger.trace({type: "payload", direction: "outgoing", request});
            try {
                const subscribe = await createExpiringPromise(
                    this.relayer.request(request),
                    this.subscribeTimeout,
                );
                await subscribe;
            } catch (err) {
                this.logger.debug(`Outgoing Relay Subscribe Payload stalled`);
                this.relayer.events.emit(RELAYER_EVENTS.connection_stalled);
            }
            return hashMessage(topic + this.clientId);
        } catch (e) {
            console.log('subscriber.rpbSubscribe error', e)
        }
    }

    private async rpcBatchSubscribe(subscriptions: SubscriberTypes.Params[]) {
        try {
            if (!subscriptions.length) return;
            const relay = subscriptions[0].relay;
            const api = getRelayProtocolApi(relay.protocol);
            const request: RequestArguments<RelayJsonRpc.BatchSubscribeParams> = {
                method: api.batchSubscribe,
                params: {
                    topics: subscriptions.map((s) => s.topic),
                },
            };
            this.logger.debug(`Outgoing Relay Payload`);
            this.logger.trace({type: "payload", direction: "outgoing", request});
            try {
                const subscribe = await createExpiringPromise(
                    this.relayer.request(request),
                    this.subscribeTimeout,
                );
                return await subscribe;
            } catch (err) {
                this.logger.debug(`Outgoing Relay Payload stalled`);
                this.relayer.events.emit(RELAYER_EVENTS.connection_stalled);
            }
        } catch (e) {
            console.log('subscriber.rpcBatchSubscribe error', e)
        }
    }

    private rpcUnsubscribe(topic: string, id: string, relay: RelayerTypes.ProtocolOptions) {
        try {
            const api = getRelayProtocolApi(relay.protocol);
            const request: RequestArguments<RelayJsonRpc.UnsubscribeParams> = {
                method: api.unsubscribe,
                params: {
                    topic,
                    id,
                },
            };
            this.logger.debug(`Outgoing Relay Payload`);
            this.logger.trace({type: "payload", direction: "outgoing", request});
            return this.relayer.request(request);
        } catch (e) {
            console.log('subscriber.rpcUnsubscribe error', e)
        }
    }

    private onSubscribe(id: string, params: SubscriberTypes.Params) {
        try {
            this.setSubscription(id, {...params, id});
            this.pending.delete(params.topic);
        } catch (e) {
            console.log('subscriber.onSubscribe error', e)
        }
    }

    private onBatchSubscribe(subscriptions: SubscriberTypes.Active[]) {
        try {
            if (!subscriptions.length) return;
            subscriptions.forEach((subscription) => {
                this.setSubscription(subscription.id, {...subscription});
                this.pending.delete(subscription.topic);
            });
        } catch (e) {
            console.log('subscriber.onBatchSubscribe error', e)
        }
    }

    private async onUnsubscribe(topic: string, id: string, reason: ErrorResponse) {
        try {
            this.events.removeAllListeners(id);
            if (this.hasSubscription(id, topic)) {
                this.deleteSubscription(id, reason);
            }
            await this.relayer.messages.del(topic);
        } catch (e) {
            console.log('subscriber.onUnsubscribe error', e)
        }
    }

    private async setRelayerSubscriptions(subscriptions: SubscriberTypes.Active[]) {
        try {
            await this.relayer.core.storage.setItem<SubscriberTypes.Active[]>(
                this.storageKey,
                subscriptions,
            );
        } catch (e) {
            console.log('subscriber.setRelayerSubscriptions error', e)
        }
    }

    private async getRelayerSubscriptions() {
        try {
            const subscriptions = await this.relayer.core.storage.getItem<SubscriberTypes.Active[]>(
                this.storageKey,
            );
            return subscriptions;
        } catch (e) {
            console.log('subscriber.getRelayerSubscriptions error', e)
        }
    }

    private setSubscription(id: string, subscription: SubscriberTypes.Active) {
        try {
            if (this.subscriptions.has(id)) return;
            this.logger.debug(`Setting subscription`);
            this.logger.trace({type: "method", method: "setSubscription", id, subscription});
            this.addSubscription(id, subscription);
        } catch (e) {
            console.log('subscriber.setSubscription error', e)
        }
    }

    private addSubscription(id: string, subscription: SubscriberTypes.Active) {
        try {
            this.subscriptions.set(id, {...subscription});
            this.topicMap.set(subscription.topic, id);
            this.events.emit(SUBSCRIBER_EVENTS.created, subscription);
        } catch (e) {
            console.log('subscriber.addSubscription error', e)
        }
    }

    private getSubscription(id: string) {
        try {
            this.logger.debug(`Getting subscription`);
            this.logger.trace({type: "method", method: "getSubscription", id});
            const subscription = this.subscriptions.get(id);
            if (!subscription) {
                const {message} = getInternalError("NO_MATCHING_KEY", `${this.name}: ${id}`);
                throw new Error(message);
            }
            return subscription;
        } catch (e) {
            console.log('subscriber.getSubscription error', e)
        }
    }

    private deleteSubscription(id: string, reason: ErrorResponse) {
        try {
            this.logger.debug(`Deleting subscription`);
            this.logger.trace({type: "method", method: "deleteSubscription", id, reason});
            const subscription = this.getSubscription(id);
            this.subscriptions.delete(id);
            this.topicMap.delete(subscription.topic, id);
            this.events.emit(SUBSCRIBER_EVENTS.deleted, {
                ...subscription,
                reason,
            } as SubscriberEvents.Deleted);
        } catch (e) {
            console.log('subscriber.deleteSubscription error', e)
        }
    }

    private restart = async () => {
        try {
            this.restartInProgress = true;
            await this.restore();
            await this.reset();
            this.restartInProgress = false;
        } catch (e) {
            console.log('subscriber.restart error', e)
        }
    };

    private async persist() {
        try {
            await this.setRelayerSubscriptions(this.values);
            this.events.emit(SUBSCRIBER_EVENTS.sync);
        } catch (e) {
            console.log('subscriber.persist error', e)
        }
    }

    private async reset() {
        try {
            if (this.cached.length) {
                const batches = Math.ceil(this.cached.length / this.batchSubscribeTopicsLimit);
                for (let i = 0; i < batches; i++) {
                    const batch = this.cached.splice(0, this.batchSubscribeTopicsLimit);
                    await this.batchSubscribe(batch);
                }
            }
            this.events.emit(SUBSCRIBER_EVENTS.resubscribed);
        } catch (e) {
            console.log('subscriber.reset error', e)
        }
        
    }

    private async restore() {
        try {
            const persisted = await this.getRelayerSubscriptions();
            if (typeof persisted === "undefined") return;
            if (!persisted.length) return;
            if (this.subscriptions.size) {
                const {message} = getInternalError("RESTORE_WILL_OVERRIDE", this.name);
                this.logger.error(message);
                this.logger.error(`${this.name}: ${JSON.stringify(this.values)}`);
                throw new Error(message);
            }
            this.cached = persisted;
            this.logger.debug(`Successfully Restored subscriptions for ${this.name}`);
            this.logger.trace({type: "method", method: "restore", subscriptions: this.values});
        } catch (e) {
            console.log('subscriber.restore error', e)
            this.logger.debug(`Failed to Restore subscriptions for ${this.name}`);
            this.logger.error(e as any);
        }
    }

    private async batchSubscribe(subscriptions: SubscriberTypes.Params[]) {
        try {
            if (!subscriptions.length) return;
            const result = (await this.rpcBatchSubscribe(subscriptions)) as string[];
            if (!isValidArray(result)) return;
            this.onBatchSubscribe(result.map((id, i) => ({...subscriptions[i], id})));
        } catch (e) {
            console.log('subscriber.batchSubscribe error', e)
        }
    }

    private async onConnect() {
        try {
            if (this.restartInProgress) return;
            await this.restart();
            this.onEnable();
        } catch (e) {
            console.log('subscriber.onConnect error', e)
        }
    }
    

    private onDisconnect() {
        try {
            this.onDisable();
        } catch (e) {
            console.log('subscriber.onDisconnect error', e)
        }
    }

    private async checkPending() {
        try {
            if (this.relayer.transportExplicitlyClosed) {
                return;
            }
            const pendingSubscriptions: SubscriberTypes.Params[] = [];
            this.pending.forEach((params) => {
                pendingSubscriptions.push(params);
            });
            await this.batchSubscribe(pendingSubscriptions);
        } catch (e) {
            console.log('subscriber.checkPending error', e)
        }
    }

    private registerEventListeners() {
        try {
            this.relayer.core.heartbeat.on(HEARTBEAT_EVENTS.pulse, async () => {
                await this.checkPending();
            });
            this.relayer.on(RELAYER_EVENTS.connect, async () => {
                await this.onConnect();
            });
            this.relayer.on(RELAYER_EVENTS.disconnect, () => {
                this.onDisconnect();
            });
            this.events.on(SUBSCRIBER_EVENTS.created, async (createdEvent: SubscriberEvents.Created) => {
                const eventName = SUBSCRIBER_EVENTS.created;
                this.logger.info(`Emitting ${eventName}`);
                this.logger.debug({type: "event", event: eventName, data: createdEvent});
                await this.persist();
            });
            this.events.on(SUBSCRIBER_EVENTS.deleted, async (deletedEvent: SubscriberEvents.Deleted) => {
                const eventName = SUBSCRIBER_EVENTS.deleted;
                this.logger.info(`Emitting ${eventName}`);
                this.logger.debug({type: "event", event: eventName, data: deletedEvent});
                await this.persist();
            });
        } catch (e) {
            console.log('subscriber.registerEventListeners error', e)
        }
    }

    private isInitialized() {
        if (!this.initialized) {
            const {message} = getInternalError("NOT_INITIALIZED", this.name);
            throw new Error(message);
        }
    }

    private async restartToComplete() {
        if (!this.restartInProgress) return;

        await new Promise<void>((resolve) => {
            const interval = setInterval(() => {
                if (!this.restartInProgress) {
                    clearInterval(interval);
                    resolve();
                }
            }, this.pollingInterval);
        });
    }
}
