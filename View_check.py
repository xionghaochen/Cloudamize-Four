'''
Created on Jun 13, 2016

@author: walter
'''
#!/usr/bin/env python3
# -*- coding:utf-8 -*-

' The first Python project '

__author__ = 'Walter Xiong'

import sys
import getopt
import psycopg2

def main(argv):
    host1=''
    dbname1=''
    port1=''
    user1=''
    password1=''
    host2=''
    dbname2=''
    port2=''
    user2=''
    password2=''
        
    try:
        opts,args=getopt.getopt(argv,"h:d:p:u:w:H:D:P:U:W",["host1=", "dbname1=", "port1=", "user1=", "password1=","host2=", "dbname2=", "port2=", "user2=", "password2="])
    except getopt.GetoptError:
        sys.exit()
        
    for key,value in opts:
        if key in ('-h','--host1'):
            host1=value
        if key in ('-d','--dbname1'):
            dbname1=value
        if key in ('-p','--port1'):
            if value=='':
                port1='5432'
            else:
                port1=value
        if key in ('-u','--user1'):
            if value=='':
                user1='postgres'
            else:
                user1=value
        if key in ('-w','--password1'):
            password1=value
        if key in ('-H','--host2'):
            host2=value
        if key in ('-D','--dbname2'):
            dbname2=value
        if key in ('-P','--port2'):
            if value=='':
                port2='5432'
            else:
                port2=value
        if key in ('-U','--user2'):
            if value=='':
                user2='postgres'
            else:
                user2=value
        if key in ('-W','--password2'):
            password2=value    
    
    connect_db(host1, dbname1, port1, user1, password1,host2, dbname2, port2, user2, password2)


def connect_db(host1, dbname1, port1, user1, password1,host2, dbname2, port2, user2, password2):
    # This function is used to connect to specified database. 
    
    conn_string1= "host=%s dbname=%s port=%s user=%s password=%s"%(host1,dbname1,port1,user1,password1)
#     print ("Connecting to database\n\n-->%s\n" %(conn_string1))
    
    conn1=psycopg2.connect(conn_string1)
    cursor1=conn1.cursor()
    
#     print ("Database %s is connected!\n"%dbname1)
    
    conn_string2= "host=%s dbname=%s port=%s user=%s password=%s"%(host2,dbname2,port2,user2,password2)
#     print ("Connecting to database\n\n-->%s\n" %(conn_string2))
    
    conn2=psycopg2.connect(conn_string2)
    cursor2=conn2.cursor()
    
#     print ("Database %s is connected!\n"%dbname2)
    
    cursor1.execute('select schema_name from information_schema.schemata where schema_name <> \'information_schema\' and schema_name !~ E\'^pg_\';')
    s_schema=cursor1.fetchall()
    
    cursor2.execute('select schema_name from information_schema.schemata where schema_name <> \'information_schema\' and schema_name !~ E\'^pg_\';')
    c_schema=cursor2.fetchall()
    
    s_schema_number,c_schema_number,schema_count=0,0,0
     
    if len(s_schema)!=len(c_schema):
        schema_count=schema_count+1
        print('The number of schemas in those two databases are not matching\n')
    else:
        while s_schema_number<len(s_schema):
            while c_schema_number<len(c_schema):
                if s_schema[s_schema_number]==c_schema[c_schema_number] and (s_schema_number+1)<len(s_schema):
                    s_schema_number=s_schema_number+1
                    c_schema_number=0
                elif s_schema[s_schema_number]==c_schema[c_schema_number] and (s_schema_number+1)>=len(s_schema):
                    s_schema_number,c_schema_number=0,0
                    break
                elif s_schema[s_schema_number]!=c_schema[c_schema_number] and (c_schema_number+1)<len(c_schema):
                    c_schema_number=c_schema_number+1
                elif s_schema[s_schema_number]!=c_schema[c_schema_number] and (c_schema_number+1)>=len(c_schema) and (s_schema_number+1)<len(s_schema):
                    schema_count=schema_count+1
                    print('The schema %r can not be found in database %r\n'%(s_schema[s_schema_number],dbname2))
                    s_schema_number=s_schema_number+1
                    c_schema_number=0
                elif s_schema[s_schema_number]!=c_schema[c_schema_number] and (c_schema_number+1)>=len(c_schema) and (s_schema_number+1)>=len(s_schema):
                    schema_count=schema_count+1
                    print('The schema %r can not be found in database %r\n'%(s_schema[s_schema_number],dbname2))
                    s_schema_number,c_schema_number=0,0
                    break
            break
        
        while c_schema_number<len(c_schema):
            while s_schema_number<len(s_schema):
                if c_schema[c_schema_number]==s_schema[s_schema_number] and (c_schema_number+1)<len(c_schema):
                    c_schema_number=c_schema_number+1
                    s_schema_number=0
                elif c_schema[c_schema_number]==s_schema[s_schema_number] and (c_schema_number+1)>=len(c_schema):
                    s_schema_number,c_schema_number=0,0
                    break
                elif c_schema[c_schema_number]!=s_schema[s_schema_number] and (s_schema_number+1)<len(s_schema):
                    s_schema_number=s_schema_number+1
                elif c_schema[c_schema_number]!=s_schema[s_schema_number] and (s_schema_number+1)>=len(s_schema) and (c_schema_number+1)<len(c_schema):
                    schema_count=schema_count+1
                    print('The schema %r can not be found in database %r\n'%(c_schema[c_schema_number],dbname1))
                    c_schema_number=c_schema_number+1
                    s_schema_number=0
                elif c_schema[c_schema_number]!=s_schema[s_schema_number] and (s_schema_number+1)>=len(s_schema) and (c_schema_number+1)>=len(c_schema):
                    schema_count=schema_count+1
                    print('The schema %r can not be found in database %r\n'%(c_schema[c_schema_number],dbname1))
                    s_schema_number,c_schema_number=0,0
                    break
            break
        
    s_schema_table_number,c_schema_table_number,schema_table_count,count=0,0,0,0
    
    if schema_count==0:
        for s in s_schema:
            s_schema_table_number,c_schema_table_number,schema_table_count=0,0,0
            cursor1.execute('select viewname as view from pg_views where schemaname = \'%s\';'%s[0])
            s_schema_table=cursor1.fetchall()
            
            cursor2.execute('select viewname as view from pg_views where schemaname = \'%s\';'%s[0])
            c_schema_table=cursor2.fetchall()
            
            if len(s_schema_table)!=len(c_schema_table):
                print('The number of views in schema %r are not matching \n'%s[0])
                count=count+1
            else:
                while s_schema_table_number<len(s_schema_table):
                    while c_schema_table_number<len(c_schema_table):
                        if s_schema_table[s_schema_table_number]==c_schema_table[c_schema_table_number] and (s_schema_table_number+1)<len(s_schema_table):
                            s_schema_table_number=s_schema_table_number+1
                            c_schema_table_number=0
                        elif s_schema_table[s_schema_table_number]==c_schema_table[c_schema_table_number] and (s_schema_table_number+1)>=len(s_schema_table):
                            s_schema_table_number,c_schema_table_number=0,0
                            break
                        elif s_schema_table[s_schema_table_number]!=c_schema_table[c_schema_table_number] and (c_schema_table_number+1)<len(c_schema_table):
                            c_schema_table_number=c_schema_table_number+1
                        elif s_schema_table[s_schema_table_number]!=c_schema_table[c_schema_table_number] and (c_schema_table_number+1)>=len(c_schema_table) and (s_schema_table_number+1)<len(s_schema_table):
                            schema_table_count=schema_table_count+1
                            print('In schema %r database %r, the view %r can not be found\n'%(s[0],dbname2,s_schema_table[s_schema_table_number][0]))
                            s_schema_table_number=s_schema_table_number+1
                            c_schema_table_number=0
                        elif s_schema_table[s_schema_table_number]!=c_schema_table[c_schema_table_number] and (c_schema_table_number+1)>=len(c_schema_table) and (s_schema_table_number+1)>=len(s_schema_table):
                            schema_table_count=schema_table_count+1
                            print('In schema %r database %r, the view %r can not be found\n'%(s[0],dbname2,s_schema_table[s_schema_table_number][0]))
                            s_schema_table_number,c_schema_table_number=0,0
                            break
                    break
                
                while c_schema_table_number<len(c_schema_table):
                    while s_schema_table_number<len(s_schema_table):
                        if c_schema_table[c_schema_table_number]==s_schema_table[s_schema_table_number] and (c_schema_table_number+1)<len(c_schema_table):
                            c_schema_table_number=c_schema_table_number+1
                            s_schema_table_number=0
                        elif c_schema_table[c_schema_table_number]==s_schema_table[s_schema_table_number] and (c_schema_table_number+1)>=len(c_schema_table):
                            s_schema_table_number,c_schema_table_number=0,0
                            break
                        elif c_schema_table[c_schema_table_number]!=s_schema_table[s_schema_table_number] and (s_schema_table_number+1)<len(s_schema_table):
                            s_schema_table_number=s_schema_table_number+1
                        elif c_schema_table[c_schema_table_number]!=s_schema_table[s_schema_table_number] and (s_schema_table_number+1)>=len(s_schema_table) and (c_schema_table_number+1)<len(c_schema_table):
                            schema_table_count=schema_table_count+1
                            print('In schema %r database %r, the view %r can not be found\n'%(s[0],dbname1,c_schema_table[c_schema_table_number][0]))
                            c_schema_table_number=c_schema_table_number+1
                            s_schema_table_number=0
                        elif c_schema_table[c_schema_table_number]!=s_schema_table[s_schema_table_number] and (s_schema_table_number+1)>=len(s_schema_table) and (c_schema_table_number+1)>=len(c_schema_table):
                            schema_table_count=schema_table_count+1
                            print('In schema %r database %r, the view %r can not be found\n'%(s[0],dbname1,c_schema_table[c_schema_table_number][0]))
                            s_schema_table_number,c_schema_table_number=0,0
                            break
                    if schema_table_count!=0:
                        count=count+1
#                         print('In schema %s, the views are not matching\n'%s[0])
                        break
                    else:
                        break
                    
    table_count=0
                
    if count!=0:
#         print('The views in those schemas are not matching\n')
        table_count=table_count+1
    elif count==0 and schema_count!=0:
        table_count=table_count+1
    elif count==0 and schema_count==0:
#         print('The view in those schemas might be matching\n')        
        for s_s in s_schema:
            cursor1.execute('select viewname as view from pg_views where schemaname = \'%s\';'%s_s[0])
            s_schema_table=cursor1.fetchall()
            for s_t in s_schema_table:
                cursor1.execute('select definition from pg_views where viewname = \'%s\' and schemaname=\'%s\';'%(s_t[0],s_s[0]))
                function1=cursor1.fetchall()
                
                cursor2.execute('select definition from pg_views where viewname = \'%s\' and schemaname=\'%s\';'%(s_t[0],s_s[0]))
                function2=cursor2.fetchall()
                
                if function1==function2:
                    continue
                else:
                    print('In schema %r, the defination of view %r in those two databases are not matching\n'%(s_s[0],s_t[0]))
                    table_count=table_count+1
                    
      
    if table_count!=0:
        print('The view of those two databases are not completely matching\n')
    else:
        print('The view of those two databases are completely matching\n')
        
                        
if __name__=='__main__':
    main(sys.argv[1:])