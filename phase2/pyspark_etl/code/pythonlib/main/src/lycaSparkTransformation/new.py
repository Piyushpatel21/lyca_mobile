
from datetime import datetime, timedelta


def hourRounder(self, t):
    # Rounds to nearest hour by adding a timedelta hour if minute >= 30
    return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
            + timedelta(hours=t.minute // 30))
def getTimeInterval(self, now):
    return now + timedelta(hours=6)

prevDate = datetime.now() + timedelta(days=-1)
run_date = prevDate.date().strftime('%Y%m%d')
batch_from = prevDate
# batch_to = lycaETL.getTimeInterval(batch_from)
