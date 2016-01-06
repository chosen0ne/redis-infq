/**
 *
 * @file    t_infq
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/03/26 18:00:51
 */

#include "redis.h"
#include "infq.h"

#include <sys/mman.h>

#define INFQ_AT_MAX_BUF_SIZE    100 * 1024

infq_dump_meta_t* fetch_infq_dump_meta(sds infq_key) {
    redisDb *db;
    robj    *qobj;
    infq_t  *q;

    db = (redisDb *)dictFetchValue(server.infq_keys, infq_key);
    if (db == NULL) {
        redisLog(REDIS_WARNING, "no db attached to infq, key: %s", infq_key);
        return NULL;
    }

    qobj = dictFetchValue(db->dict, infq_key);
    if (qobj == NULL || qobj->type != REDIS_INFQ) {
        redisLog(REDIS_WARNING, "no object or not a infq obj specified by key, key: %s", infq_key);
        return NULL;
    }

    q = (infq_t *)qobj->ptr;
    return infq_fetch_dump_meta(q);
}

unsigned long infqLength(robj *q) {
    if (q->encoding == REDIS_ENCODING_INFQ) {
        return infq_size(q->ptr);
    } else {
        redisPanic("Not a infQ");
    }
}

robj* createInfQ(robj *key, redisDb *db) {
    robj        *q;
    dictEntry   *de;

    q = createInfqObject(key);
    if (q == NULL) {
        redisLog(REDIS_WARNING, "failed to create InfQ, key: %s", (sds)key->ptr);
        return NULL;
    }

    // add InfQ to DB
    dbAdd(db, key, q);

    // NOTICE: dbAdd时会拷贝key，此处复用该key
    de = dictFind(db->dict, key->ptr);
    dictReplace(server.infq_keys, dictGetKey(de), db);

    return q;
}

int pushObj(robj *qobj, robj *val) {
    sds     s;
    rio     r;
    int     data_size, ret;
    size_t  size;
    void    *raw_data;

    // serialize robj to raw buffer
    s = sdsempty();
    rioInitWithBuffer(&r, s);
    data_size = rdbSaveObject(&r, val);

    // NOTICE: memory address of sds is changed when the space is increased
    s = r.io.buffer.ptr;
    redisAssert((size_t)data_size == sdslen(s));

    // fetch the start pointer which point to the sdshdr and the length of sdshdr and data
    sdsraw(s, &raw_data, &size);
    // NOTICE: avoid the copy from robj => buffer
    ret = REDIS_OK;
    if (infq_push(qobj->ptr, raw_data, size) == INFQ_ERR) {
        redisLog(REDIS_WARNING, "failed to push infq, data: %s, len: %d", s, data_size);
        ret = REDIS_ERR;
    }

    if (s != NULL) {
        sdsfree(s);
    }

    return ret;
}

robj* deserialize(const void *dataptr, int size) {
    sds     s;
    rio     r;
    robj    *obj;

    s = sdsinit(dataptr, size);
    if (s == NULL) {
        redisLog(REDIS_WARNING, "failed to convert raw buffer to sds, size: %d", size);
        return NULL;
    }

    // deserialize buffer to robj
    rioInitWithBuffer(&r, s);
    obj = rdbLoadObject(REDIS_RDB_TYPE_STRING, &r);
    if (obj == NULL) {
        redisLog(REDIS_WARNING, "failed to load from buffer");
        return NULL;
    }

    return obj;
}

/*-----------------------------------------------------------------------------
 * infQ Commands
 *
 * Aims on replcaing most commands of Lists.
 * qpush => lpush
 * qpop  => rpop
 * qlen  => llen
 * qat   => lindex
 * qrange => lranage
 * qpushx => lpushx
 * qrpoplpush => rpoplpush
 *----------------------------------------------------------------------------*/

void qpushCommand(redisClient *c) {
    int         j, pushed;
    robj        *qobj;
    dictEntry   *de;

    pushed = 0;
    qobj = lookupKeyWrite(c->db, c->argv[1]);

    if (qobj && qobj->type != REDIS_INFQ) {
        addReply(c, shared.wrongtypeerr);
        return;
    }

    for (j = 2; j < c->argc; j++) {
        c->argv[j] = tryObjectEncoding(c->argv[j]);
        if (!qobj) {
            de = dictFind(server.infq_keys, c->argv[1]->ptr);
            redisAssert(de == NULL);

            qobj = createInfQ(c->argv[1], c->db);
            if (qobj == NULL) {
                addReplyError(c, "failed to create infq");
                return;
            }
        }

        if (pushObj(qobj, c->argv[j]) == REDIS_ERR) {
            redisLog(REDIS_WARNING, "failed to push InfQ, key: %s", (sds)c->argv[1]->ptr);
            addReplyErrorFormat(c, "failed to push infq");
            return;
        }

        pushed++;
    }
    addReplyLongLong(c, qobj ? infqLength(qobj) : 0);
    server.dirty += pushed;
}

void qpushxCommand(redisClient *c) {
    robj    *qobj;

    if ((qobj = lookupKeyReadOrReply(c, c->argv[1], shared.czero)) == NULL ||
            checkType(c, qobj, REDIS_INFQ)) {
        return;
    }

    c->argv[2] = tryObjectEncoding(c->argv[2]);
    if (pushObj(qobj, c->argv[2]) == REDIS_ERR) {
        redisLog(REDIS_WARNING, "failed to push InfQ, key: %s", (sds)c->argv[1]->ptr);
        addReplyErrorFormat(c, "failed to push infq");
        return;
    }

    addReplyLongLong(c, infqLength(qobj));
    server.dirty += 1;
}

void qpopCommand(redisClient *c) {
    const void  *dataptr;
    int         size;
    robj        *obj, *q;

    q = lookupKeyWriteOrReply(c, c->argv[1], shared.nullbulk);
    if (q == NULL || checkType(c, q, REDIS_INFQ)) {
        redisLog(REDIS_WARNING, "val is NULL or not a InfQ");
        return;
    }

    if (infq_pop_zero_cp(q->ptr, &dataptr, &size) == INFQ_ERR) {
        redisLog(REDIS_WARNING, "failed to pop from infq, key: %s", (char *)c->argv[1]->ptr);
        addReplyError(c, "failed to pop from infq");
        return;
    }

    if (size == 0) {
        addReply(c, shared.nullbulk);
        return;
    }

    if ((obj = deserialize(dataptr, size)) == NULL) {
        addReplyError(c, "failed to deserialize");
        return;
    }

    addReplyBulk(c, obj);
    decrRefCount(obj);
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
    int             data_size;

    q = lookupKeyWriteOrReply(c, c->argv[1], shared.nullbulk);
    if (q == NULL || checkType(c, q, REDIS_INFQ)) {
        return;
    }

    if (infq_top_zero_cp(q->ptr, &data, &data_size) == INFQ_ERR) {
        redisLog(REDIS_WARNING, "failed to fetch top from infq, key: %s",
                (char *)c->argv[1]->ptr);
        addReplyError(c, "failed to fetch top from infq");
        return;
    }

    if (data_size == 0) {
        addReply(c, shared.nullbulk);
        return;
    }

    if ((obj = deserialize(data, data_size)) == NULL) {
        addReplyError(c, "failed to deserialize");
        return;
    }

    addReplyBulk(c, obj);
    decrRefCount(obj);
}

void qjpopCommand(redisClient *c) {
    robj    *q;

    q = lookupKeyWriteOrReply(c, c->argv[1], shared.nullbulk);
    if (q == NULL || checkType(c, q, REDIS_INFQ)) {
        return;
    }

    if (infq_just_pop(q->ptr) == INFQ_ERR) {
        redisLog(REDIS_WARNING, "failed to just pop from infq, key: %s", (char *)c->argv[1]->ptr);
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

    if (dbDelete(c->db, c->argv[1])) {
        server.dirty++;
    }

    addReplyBulk(c, shared.ok);
    server.dirty++;
}

void qatCommand(redisClient *c) {
    char*       data[INFQ_AT_MAX_BUF_SIZE];
    int         data_size;
    robj        *q, *obj;
    long        idx, qlen;

    if (getLongFromObjectOrReply(c, c->argv[2], &idx, "index must be a integer") != REDIS_OK) {
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

    if (infq_at(q->ptr, idx, &data, INFQ_AT_MAX_BUF_SIZE, &data_size) == INFQ_ERR) {
        addReplyError(c, "failed to call at");
        sds key = c->argv[1]->ptr;
        redisLog(REDIS_WARNING, "failed to call at of InfQ, key: %s, size: %ld, idx: %ld",
                key, qlen, idx);
        return;
    }

    if (data_size == 0) {
        addReply(c, shared.nullbulk);
        return;
    }

    if ((obj = deserialize(data, data_size)) == NULL) {
        addReplyError(c, "failed to deserialize");
        return;
    }

    addReplyBulk(c, obj);
    decrRefCount(obj);
}

void qrangeCommand(redisClient *c) {
    const void  *data;
    int         data_size;
    robj        *q, *obj;
    long        qlen, start, end, rangelen;

    if ((getLongFromObjectOrReply(c, c->argv[2], &start, "start must be a integer") == REDIS_ERR)
            || (getLongFromObjectOrReply(c, c->argv[3], &end, "end must be a integer") == REDIS_ERR)) {
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
            addReply(c, shared.nullbulk);
            sds key = c->argv[1]->ptr;
            redisLog(REDIS_WARNING, "failed to fetch range of InfQ, key: %s, "
                    "range: [%ld, %ld], idx: %d", key, start, end, i);
            continue;
        }

        if (data_size == 0) {
            addReply(c, shared.nullbulk);
            continue;
        }

        if ((obj = deserialize(data, data_size)) == NULL) {
            redisLog(REDIS_WARNING, "failed to deserialize");
            addReply(c, shared.nullbulk);
            continue;
        }

        addReplyBulk(c, obj);
        decrRefCount(obj);
    }
}

// pop from InfQ, push to List
void qpopLpushGeneric(redisClient *c, int where) {
    const void  *dataptr;
    robj        *sobj, *value, *dobj, *touchedkey;
    int         size;

    // find InfQ
    if ((sobj = lookupKeyWriteOrReply(c, c->argv[1], shared.nullbulk)) == NULL ||
            checkType(c, sobj, REDIS_INFQ)) {
        return;
    }

    if (infq_size(sobj->ptr) == 0) {
        addReply(c, shared.nullbulk);
        return;
    }

    dobj = lookupKeyWrite(c->db, c->argv[2]);
    touchedkey = c->argv[1];

    if (dobj && checkType(c, dobj, REDIS_LIST)) {
        return;
    }

    // pop data
    if (infq_pop_zero_cp(sobj->ptr, &dataptr, &size) == INFQ_ERR) {
        redisLog(REDIS_WARNING, "failed to pop from infq, key: %s", (char *)touchedkey->ptr);
        addReplyError(c, "failed to pop from InfQ");
        return;
    }

    if (size == 0) {
        addReply(c, shared.nullbulk);
        redisLog(REDIS_WARNING, "InfQ is empty when pop, key: %s", (char *)touchedkey->ptr);
        return;
    }

    if ((value = deserialize(dataptr, size)) == NULL) {
        addReplyError(c, "failed to pop from InfQ");
        return;
    }

    incrRefCount(touchedkey);
    if (dobj == NULL) {
        dobj = createZiplistObject();
        dbAdd(c->db, c->argv[2], dobj);
    }
    signalModifiedKey(c->db, c->argv[2]);

    listTypePush(dobj, value, where);
    if (where == REDIS_TAIL) {
        notifyKeyspaceEvent(REDIS_NOTIFY_LIST, "rpush", c->argv[2], c->db->id);
    } else {
        notifyKeyspaceEvent(REDIS_NOTIFY_LIST, "lpush", c->argv[2], c->db->id);
    }

    addReplyBulk(c, value);

    decrRefCount(value);
    signalModifiedKey(c->db, touchedkey);
    decrRefCount(touchedkey);
    server.dirty++;
}

void qpoprpushCommand(redisClient *c) {
    qpopLpushGeneric(c, REDIS_TAIL);
}

void qpoplpushCommand(redisClient *c) {
    qpopLpushGeneric(c, REDIS_HEAD);
}

// pop from List, push to List
void lpopQpushGeneric(redisClient *c, int where) {
    robj        *sobj, *value;
    dictEntry   *de;

    if ((sobj = lookupKeyReadOrReply(c, c->argv[1], shared.nullbulk)) == NULL ||
            checkType(c, sobj, REDIS_LIST)) {
        return;
    }

    if (listTypeLength(sobj) == 0) {
        addReply(c, shared.nullbulk);
        return;
    }

    robj *qobj = lookupKeyWrite(c->db, c->argv[2]);
    robj *touchedkey = c->argv[1];

    if (qobj && checkType(c, qobj, REDIS_INFQ)) {
         return;
    }

    // create InfQ if needed
    if (qobj == NULL) {
        de = dictFind(server.infq_keys, c->argv[2]->ptr);
        redisAssert(de == NULL);

        qobj = createInfQ(c->argv[2], c->db);
        if (qobj == NULL) {
             addReplyError(c, "failed to create infq");
             return;
        }
    }

    value = listTypePop(sobj, where);
    incrRefCount(touchedkey);

    if (pushObj(qobj, value) == REDIS_ERR) {
         redisLog(REDIS_WARNING, "failed to pop list and push InfQ, key: %s, where: %d",
                 (sds)c->argv[2]->ptr, where);
         addReplyErrorFormat(c, "failed to push infq");

         // push value back to list
         listTypePush(sobj, value, where);
         return;
    }

    decrRefCount(value);
    /* Delete the source list when it is empty */
    notifyKeyspaceEvent(REDIS_NOTIFY_LIST,"rpop",touchedkey,c->db->id);
    if (listTypeLength(sobj) == 0) {
        dbDelete(c->db,touchedkey);
        notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",
                            touchedkey,c->db->id);
    }

    addReplyBulk(c, value);
    signalModifiedKey(c->db,touchedkey);
    decrRefCount(touchedkey);
    server.dirty++;
}

void rpopqpushCommand(redisClient *c) {
    lpopQpushGeneric(c, REDIS_TAIL);
}

void lpopqpushCommand(redisClient *c) {
    lpopQpushGeneric(c, REDIS_HEAD);
}

void qrpoplpushCommand(redisClient *c) {
    robj        *sobj, *dobj, *obj;
    dictEntry   *de;
    const void  *dataptr;
    int         size;

    if ((sobj = lookupKeyReadOrReply(c, c->argv[1], shared.nullbulk)) == NULL ||
            checkType(c, sobj, REDIS_INFQ)) {
        return;
    }

    // source queue is empty
    if (infq_size(sobj->ptr) == 0) {
        addReply(c, shared.nullbulk);
        return;
    }

    dobj = lookupKeyWrite(c->db, c->argv[2]);
    if (dobj && checkType(c, dobj, REDIS_INFQ)) {
        return;
    }

    // create infq if needed
    if (dobj == NULL) {
        de = dictFind(server.infq_keys, c->argv[2]->ptr);
        redisAssert(de == NULL);

        dobj = createInfQ(c->argv[2], c->db);
        if (dobj == NULL) {
            redisLog(REDIS_WARNING, "failed to create InfQ");
            addReplyError(c, "failed to create InfQ");
            return;
        }
    }

    if (infq_top_zero_cp(sobj->ptr, &dataptr, &size) == INFQ_ERR) {
        redisLog(REDIS_WARNING, "failed to fetch top from infq, key: %s", (sds)c->argv[1]->ptr);
        addReplyError(c, "failed to fetch pop from infq");
        return;
    }

    if (size == 0) {
        addReply(c, shared.nullbulk);
        return;
    }

    if ((obj = deserialize(dataptr, size)) == NULL) {
        addReplyError(c, "failed to deserial");
        return;
    }

    if (infq_push(dobj->ptr, (void *)dataptr, size) == INFQ_ERR) {
        redisLog(REDIS_WARNING, "failed to push infq, key: %s", (sds)c->argv[1]->ptr);
        addReplyError(c, "failed to push infq");
        return;
    }

    if (infq_just_pop(sobj->ptr) == INFQ_ERR) {
        redisLog(REDIS_WARNING, "failed to just pop from infq, key: %s", (sds)c->argv[1]->ptr);
        addReplyError(c, "failed to just pop");
        return;
    }

    addReplyBulk(c, obj);
    server.dirty++;
}

void qinspectCommand(redisClient *c) {
    const char      *debug_info;
    char            buf[2048];
    robj            *qobj;

    qobj = lookupKeyReadOrReply(c, c->argv[1], shared.nullbulk);
    if (qobj == NULL || checkType(c, qobj, REDIS_INFQ)) {
        return;
    }

    if ((debug_info = infq_debug_info(qobj->ptr, buf, 2048)) == NULL) {
        addReply(c, shared.nullbulk);
        return;
    }

    addReplyBulkCString(c, buf);
}

