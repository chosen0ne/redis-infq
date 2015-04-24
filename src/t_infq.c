/**
 *
 * @file    t_infq
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/03/26 18:00:51
 */

#include "redis.h"
#include "infq.h"

unsigned long infqLength(robj *q) {
    if (q->encoding == REDIS_ENCODING_INFQ) {
        return infq_size(q->ptr);
    } else {
        redisPanic("Not a infQ");
    }
}

/*-----------------------------------------------------------------------------
 * infQ Commands
 *----------------------------------------------------------------------------*/

void qpushCommand(redisClient *c) {
    int         j, pushed, data_size;
    sds         s;
    rio         r;
    void        *raw_data;
    size_t      size;
    robj        *qobj;

    pushed = 0;
    qobj = lookupKeyWrite(c->db, c->argv[1]);

    if (qobj && qobj->type != REDIS_INFQ) {
        addReply(c, shared.wrongtypeerr);
        return;
    }

    for (j = 2; j < c->argc; j++) {
        c->argv[j] = tryObjectEncoding(c->argv[j]);
        if (!qobj) {
            qobj = createInfqObject(c->argv[1]);
            if (qobj == NULL) {
                addReplyError(c, "failed to create infq");
                return;
            }
            dbAdd(c->db,c->argv[1],qobj);
            server.infq_key = sdsdup(c->argv[1]->ptr);
            server.infq_db = c->db;
        }

        // push to infq
        // serialize robj to raw buffer
        s = sdsempty();
        rioInitWithBuffer(&r, s);
        data_size = rdbSaveObject(&r, c->argv[j]);

        // NOTICE: memory address of sds is changed when the space is increased
        s = r.io.buffer.ptr;
        redisAssert((size_t)data_size == sdslen(s));

        // fetch the start pointer which point to the sdshdr and the length of sdshdr and data
        sdsraw(s, &raw_data, &size);
        // NOTICE: avoid the copy from robj => buffer
        if (infq_push(qobj->ptr, raw_data, size) == INFQ_ERR) {
            redisLog(REDIS_WARNING, "failed to push infq, data: %s, len: %d",
                    s, data_size);
            addReplyErrorFormat(c, "failed to push infq");
            return;
        }

        if (s != NULL) {
            sdsfree(s);
        }
        pushed++;
    }
    addReplyLongLong(c, qobj ? infqLength(qobj) : 0);
    server.dirty += pushed;
}

void qpopCommand(redisClient *c) {
    const void  *dataptr;
    sds         s;
    rio         r;
    int         size;
    robj        *obj, *q;

    q = lookupKeyWriteOrReply(c, c->argv[1], shared.nullbulk);
    if (q == NULL || checkType(c, q, REDIS_INFQ)) {
        return;
    }

    if (infq_pop_zero_cp(q->ptr, &dataptr, &size) == INFQ_ERR) {
        redisLog(REDIS_WARNING, "failed to pop from infq, key: %s", c->argv[1]->ptr);
        addReplyError(c, "failed to pop from infq");
        return;
    }

    if (size == 0) {
        addReply(c, shared.nullbulk);
    }

    s = sdsinit(dataptr, size);
    if (s == NULL) {
        redisLog(REDIS_WARNING, "failed to convert raw buffer to sds in qpop");
        addReplyError(c, "failed to pop from infq");
        return;
    }

    // deserialize buffer to robj
    rioInitWithBuffer(&r, s);
    obj = rdbLoadObject(REDIS_RDB_TYPE_STRING, &r);
    if (obj == NULL) {
        redisLog(REDIS_WARNING, "failed to deserialize");
        addReplyError(c, "failed to deserialize");
        return;
    }

    addReplyBulk(c, obj);
    server.dirty++;
}

void qlenCommand(redisClient *c) {
    robj    *q;
    size_t  len;

    q = lookupKeyWriteOrReply(c, c->argv[1], shared.nullbulk);
    if (q == NULL || checkType(c, q, REDIS_INFQ)) {
        return;
    }

    len = infq_size(q->ptr);

    addReplyLongLong(c, len);
}

void qtopCommand(redisClient *c) {
    const void      *data;
    robj            *q, *obj;
    sds             s;
    int             data_size;
    rio             r;

    q = lookupKeyWriteOrReply(c, c->argv[1], shared.nullbulk);
    if (q == NULL || checkType(c, q, REDIS_INFQ)) {
        return;
    }

    if (infq_top_zero_cp(q->ptr, &data, &data_size) == INFQ_ERR) {
        redisLog(REDIS_WARNING, "failed to fetch top from infq, key: %s", c->argv[1]->ptr);
        addReplyError(c, "failed to fetch top from infq");
        return;
    }

    s = sdsinit(data, data_size);
    if (s == NULL) {
        redisLog(REDIS_WARNING, "failed to convert raw buffer to sds in qtop");
        addReplyError(c, "failed to fetch pop from infq");
        return;
    }

    // deserialize buffer to robj
    rioInitWithBuffer(&r, s);
    obj = rdbLoadObject(REDIS_RDB_TYPE_STRING, &r);
    if (obj == NULL) {
        redisLog(REDIS_WARNING, "failed to deserialize");
        addReplyError(c, "failed to deserialize");
        return;
    }

    addReplyBulk(c, obj);
}

void qjpopCommand(redisClient *c) {
    robj    *q;

    q = lookupKeyWriteOrReply(c, c->argv[1], shared.nullbulk);
    if (q == NULL || checkType(c, q, REDIS_INFQ)) {
        return;
    }

    if (infq_just_pop(q->ptr) == INFQ_ERR) {
        redisLog(REDIS_WARNING, "failed to just pop from infq, key: %s", c->argv[1]->ptr);
        addReplyError(c, "failed to jus pop from infq");
        return;
    }

    addReplyBulk(c, shared.ok);
    server.dirty++;
}

void qdelCommand(redisClient *c) {
    robj    *q;

    q = lookupKeyWriteOrReply(c, c->argv[1], shared.nullbulk);
    if (q == NULL || checkType(c, q, REDIS_INFQ)) {
        return;
    }

    infq_destroy(q->ptr);
    sdsfree(server.infq_key);
    server.infq_key = NULL;
    server.infq_db = NULL;

    addReplyBulk(c, shared.ok);
    server.dirty++;
}

void qatCommand(redisClient *c) {
    const void  *data;
    int         data_size;
    robj        *q, *obj;
    long        idx, qlen;
    sds         s;
    rio         r;

    if (getLongFromObjectOrReply(c, c->argv[2], &idx, NULL) != REDIS_OK) {
        return;
    }

    q = lookupKeyWriteOrReply(c, c->argv[1], shared.nullbulk);
    if (q == NULL || checkType(c, q, REDIS_INFQ)) {
        return;
    }

    // convert negative index to positive
    qlen = infq_size(q->ptr);
    if (idx < 0) {
        idx = qlen + idx;
    }

    // invalid index
    if (idx >= qlen) {
        addReply(c, shared.emptymultibulk);
        return;
    }

    if (infq_at_zero_cp(q->ptr, idx, &data, &data_size) == INFQ_ERR) {
        addReplyError(c, "failed to call at");
        sds key = c->argv[1]->ptr;
        redisLog(REDIS_WARNING, "failed to call at of InfQ, key: %s, size: %ld, idx: %ld",
                key, qlen, idx);
        return;
    }

    s = sdsinit(data, data_size);
    if (s == NULL) {
        addReplyError(c, "failed to convert raw buffer to robj");
        sds key = c->argv[1]->ptr;
        redisLog(REDIS_WARNING, "failed to convert raw buffer to robj, key: %s", key);
        return;
    }

    rioInitWithBuffer(&r, s);
    obj = rdbLoadObject(REDIS_RDB_TYPE_STRING, &r);
    if (obj == NULL) {
        redisLog(REDIS_WARNING, "failed to deserialize");
        addReplyError(c, "failed to deserialize");
        return;
    }

    addReplyBulk(c, obj);
}

void qrangeCommand(redisClient *c) {
    const void  *data;
    int         data_size;
    robj        *q, *obj;
    long        qlen, start, end, rangelen;
    sds         s;
    rio         r;

    if ((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) == REDIS_ERR)
            || (getLongFromObjectOrReply(c, c->argv[3], &end, NULL) == REDIS_ERR)) {
        return;
    }

    q = lookupKeyWriteOrReply(c, c->argv[1], shared.nullbulk);
    if (q == NULL || checkType(c, q, REDIS_INFQ)) {
        return;
    }

    // conver negative indexes
    qlen = infq_size(q->ptr);
    if (start < 0) {
        start = qlen + start;
    }
    if (end < 0) {
        end = qlen + end;
    }
    if (start < 0) {
        start = 0;
    }

    if (start > end || start >= qlen) {
        addReply(c, shared.emptymultibulk);
        return;
    }

    if (end >= qlen) {
        end = qlen - 1;
    }
    rangelen = end - start + 1;

    addReplyMultiBulkLen(c, rangelen);
    for (int i = start; i <= end; i++) {
        if (infq_at_zero_cp(q->ptr, i, &data, &data_size) == INFQ_ERR) {
            addReplyErrorFormat(c, "failed to fetch range data at %d", i);
            sds key = c->argv[1]->ptr;
            redisLog(REDIS_WARNING, "failed to fetch range of InfQ, key: %s, "
                    "range: [%ld, %ld], idx: %d", key, start, end, i);
            return;
        }

        s = sdsinit(data, data_size);
        if (s == NULL) {
            addReplyErrorFormat(c, "failed to fetch range data, cannot convert raw buffer");
            sds key = c->argv[1]->ptr;
            redisLog(REDIS_WARNING, "failed to fetch range of InfQ, key: %s, "
                    "range: [%ld, %ld], idx: %d", key, start, end, i);
            return;
        }
        rioInitWithBuffer(&r, s);
        obj = rdbLoadObject(REDIS_RDB_TYPE_STRING, &r);
        addReplyBulk(c, obj);
    }
}
