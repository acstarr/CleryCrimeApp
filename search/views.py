from django.shortcuts import render
import psycopg2
from django.http import HttpResponse, JsonResponse
from django.conf import settings
import sys
import json
from datetime import datetime

def daysToRange(min, max):
  if max < min:
    return tuple(list(range(1,max+1))+list(range(min, 32)) )
  else:
    return tuple(range (min,max+1))


def str_to_bool(s):
  s = s.lower()
  if s == 'true':
    return True
  elif s == 'false':
    return False
  else:
    raise ValueError # evil ValueError that doesn't tell you what the wrong value was
# Create your views here.
def search(request):
    # if request.method == 'GET':
    #     #select everything
    #     return HttpResponse("hi")
    # el
    if request.method == 'POST':
        #select based off parameters
        #parameters = request.POST
        parameters_unicode = request.body.decode('utf-8', 'replace')
        parameters = json.loads(parameters_unicode)
        #print(parameters)
        #sys.stdout.flush()

        db = psycopg2.connect(user=settings.DATABASES['default']['USER'],password=settings.DATABASES['default']['PASSWORD'],dbname=settings.DATABASES['default']['NAME'],host=settings.DATABASES['default']['HOST'])
        c = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        query = c.mogrify("""
            SELECT * 
            FROM Incidents as I
            LEFT JOIN Building as B ON B.locID = I.location
            LEFT JOIN  IsOn ON IsOn.building = I.location 
            LEFT JOIN Campus as C ON IsOn.Campus = C.locID
            LEFT JOIN Status as S ON I.incidentID = S.incident
            LEFT JOIN Dispositions as Disp ON S.disposition = Disp.dID 
            LEFT JOIN Location as L ON  I.location = L.locID 
            LEFT JOIN Violations as V ON I.violation = V.violationID 
            LEFT JOIN Days as D1 ON I.startDay = D1.day
            WHERE CASE WHEN  %(campus)s IS NOT NULL THEN C.locID = ANY (%(campus)s) ELSE FALSE END 
                AND CASE WHEN %(years)s IS NOT NULL THEN EXTRACT(YEAR FROM D1.day) = ANY (%(years)s) ELSE FALSE END   
                AND CASE WHEN %(mnth)s IS NOT NULL THEN EXTRACT(MONTH FROM D1.day) = ANY (%(mnth)s)  ELSE FALSE END    
                AND EXTRACT(DAY FROM D1.day) in %(dayRange)s 
                AND CASE WHEN %(day)s IS NOT NULL THEN D1.dayOfWeek = ANY (%(day)s) ELSE FALSE END   
                AND CASE WHEN %(startTime)s <= %(endTime)s THEN I.startTime >= %(startTime)s AND I.startTime <= %(endTime)s 
                                                           ELSE I.startTime >= %(startTime)s OR I.startTime <= %(endTime)s END
                AND CASE WHEN %(crime)s IS NOT NULL THEN I.violation = ANY (%(crime)s) ELSE FALSE END 
                AND CASE WHEN %(disposition)s IS NOT NULL THEN Disp.dID = ANY (%(disposition)s) ELSE FALSE END
                AND D1.tmax >= %(maxTempMin)s
                AND D1.tmax <= %(maxTempMax)s
                AND D1.tmin >= %(minTempMin)s
                AND D1.tmin <= %(minTempMax)s
                AND D1.tavg >= %(avgTempMin)s
                AND D1.tavg <= %(avgTempMax)s
                AND D1.prcp >= %(precipMin)s
                AND D1.prcp <= %(precipMax)s
                AND CASE WHEN  B.isDorm IS NOT NULL THEN (CASE WHEN %(dorm)s THEN B.isDorm ELSE TRUE END
                                    AND CASE WHEN %(accedemic)s THEN B.isAccedemic ELSE TRUE END 
                                    AND CASE WHEN %(dining)s THEN B.isDining ELSE TRUE END 
                                    AND CASE WHEN %(sports)s THEN B.isSports ELSE TRUE END )
                         ELSE TRUE END
                                ;

            """,{'campus': list(map(int,parameters['campus'])) or None,'mnth': list(parameters['mnth']) or None, 'day': list(parameters['day']) or None, 
                 'startTime': datetime.strptime(parameters['times'][0], '%H:%M:%S').time(),
                 'endTime': datetime.strptime(parameters['times'][1], '%H:%M:%S').time(),'crime':list(parameters['crime']) or None,
                 'disposition':list(map(int,parameters['disposition'])) or None,'maxTempMin':parameters['maxTemp'][0],
                 'maxTempMax':parameters['maxTemp'][1],'minTempMin':parameters['minTemp'][0],'minTempMax':parameters['minTemp'][1],
                 'avgTempMin':parameters['avgTemp'][0],'avgTempMax':parameters['avgTemp'][1],'precipMin':parameters['precip'][0],
                 'precipMax':parameters['precip'][1],'dayRange':daysToRange(parameters['days'][0],parameters['days'][1]),
                 'dorm':(parameters['building'][0]),'accedemic':(parameters['building'][1]),'dining':(parameters['building'][2]),
                 'sports':(parameters['building'][3]),'years': list(parameters['year']) or None}) #TODO add other times
        #print(query)
        #sys.stdout.flush()
        c.execute(query)
        data = c.fetchall()
        #print (len(data))
        #sys.stdout.flush()
        return JsonResponse(data,safe=False)