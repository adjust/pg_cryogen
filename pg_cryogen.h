#ifndef __PG_CRYOGEN_H__
#define __PG_CRYOGEN_H__

#include "access/tableam.h"
#include "utils/snapshot.h"


extern const TableAmRoutine CryoAmRoutine;

extern bool xid_is_visible(Snapshot snapshot, TransactionId xid);

#endif
