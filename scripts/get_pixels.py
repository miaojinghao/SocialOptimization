#! /usr/bin/env python

import MySQLdb
import sys

def main():
    db_host = "adopsdb1001.east.sharethis.com"
    db_user = "sharethis"
    db_passwd = "sharethis"

    db = MySQLdb.connect(host=db_host, user=db_user, passwd=db_passwd)
    cur = db.cursor()
    sql = "select distinct trim(a.rtName), e.networkCpgId, b.type \
           from adplatform.audience a, adplatform.audienceGroup b, adplatform.mappingAdGrpAndAudGrp c, adplatform.mappingAudGrpAndAud d, adplatform.adgroup e \
           where b.type in ('1', '7') \
           and b.id = c.audGrpId and c.audGrpId = d.audGrpId and c.adGroupId = e.id and d.audId = a.id \
           and d.audGrpId != '0' and a.rtName is not NULL and trim(a.rtName) != 'NULL' and trim(a.rtName) != ''"
    
    # from_unixtime(f.endDate) >= date_sub(now(), interval 30 day) and

    sys.stderr.write(sql + "\n")
    cur.execute(sql)
    res = []
    for row in cur.fetchall():
        if len(row) >= 3:
            res.append(row[0] + "|" + str(row[1]) + "|" + str(row[2]))
    # for moat 9999, need manual mapping with type = 8
    res.append("1000000|1000000|8")
    print "Pixels=" + ",".join(res)
    

if __name__ == "__main__":
    main()
