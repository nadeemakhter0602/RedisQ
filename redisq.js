import {
    createClient
} from "redis";

class RedisQ {
    constructor(redisURI, qName) {
        this.redisConn = createClient({
            url: redisURI,
        });
        this.mainQ = qName;
        this.processingQ = qName + ":processing";
    }

    async connect() {
        this.redisConn.on('error', err => console.log('Redis Client Error', err));
        await this.redisConn.connect();
    }

    async disconnect() {
        await this.redisConn.disconnect();
    }

    async mainQSize() {
        const mainQLen = await this.redisConn.lLen(this.mainQ);
        return mainQLen;
    }

    async processingQSize() {
        const processingQLen = await this.redisConn.lLen(this.processingQ);
        return processingQLen;
    }

    async get(block = true) {
        if (block) {
            const item = await this.redisConn.blMove(this.mainQ, this.processingQ, "RIGHT", "LEFT");
        } else {
            const item = await this.redisConn.lMove(this.mainQ, this.processingQ, "RIGHT", "LEFT");
        }
    }

    async complete(item) {
        await this.redisConn.lRem(this.processingQ, -1, item);
    }
}