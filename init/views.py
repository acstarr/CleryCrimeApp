from django.http import HttpResponse, FileResponse, JsonResponse
import os
import sys
import json
import psycopg2
from django.conf import settings



def index(request):
    if request.method == 'GET':
        homepage = os.getcwd()+'/cecilsafe'
        #print(homepage)
        return FileResponse(open(homepage,'rb'))
    elif request.method == 'POST':
        db=psycopg2.connect(user=settings.DATABASES['default']['USER'],password=settings.DATABASES['default']['PASSWORD'],dbname=settings.DATABASES['default']['NAME'],host=settings.DATABASES['default']['HOST'])
        c=db.cursor()
        c.execute("SELECT V.violationID, V.code, V.description, CT.type FROM Violations as V LEFT JOIN Types as T  ON V.violationID = T.violationID LEFT JOIN CrimeTypes as CT ON CT.typeID = T.typeID;",)
        data = c.fetchall()
        #data = [x[0] for x in data]
        #print(data)
        data = [dict(zip(["violationID", "code", "description", "type"],row)) for row in data]
        reform = {}
        for d in data:
            if d['type'] in reform :
                reform[d['type']].append(d)
            else :
                reform[d['type']] = [d]
        #print(reform)
        #print("homepage")
        #sys.stdout.flush()
        c.execute("SELECT * FROM Dispositions;")
        response = c.fetchall()
        final_data = {}
        final_data['dispotions'] = response
        final_data['crime_types'] = reform

        #print(final_data)
        #sys.stdout.flush()

        return JsonResponse(final_data, safe=False)

def about(request):
    if request.method == 'GET':
        aboutpage = os.getcwd()+'/about'
        #print(aboutpage)
        return FileResponse(open(aboutpage,'rb'))

#
# db=psycopg2.connect(user=settings.DATABASES['default']['USER'],passwd=settings.DATABASES['default']['PASSWORD'],db=settings.DATABASES['default']['NAME']
# ,host=settings.DATABASES['default']['HOST'])
# c=db.cursor()
# grad=1
# c.execute("SELECT * from Location where locID = 1",)
# print(c.fetchall())
