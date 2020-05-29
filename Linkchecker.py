
#################################################################################################################
#################################################################################################################
#################################################################################################################
#################################################################################################################
#################################################################################################################
from bs4 import BeautifulSoup
import subprocess
import traceback
import threading
import requests
import winsound
import sqlite3
import random
import json
import time
import sys
import csv
import os

#################################################################################################################
#################################################################################################################
#################################################################################################################
#################################################################################################################
#################################################################################################################
class cls_thread():
    def __init__(self ,thread_name="-1" ,pxy={}):
        self.t1=None
        self.cancel=0
        self.tname=thread_name
        self.processed=-1
        self.start_time=-1
        self.end_time=-1
        self.proxy=pxy
        self.cweb=None
        self.links=[]
    def __del__(self):
        self.stop_thread()
        del self.t1
        del self.cweb
        del self.links
        del self.cancel
        del self.tname
        del self.start_time
        del self.end_time

#---------------------------------------------------------------------------------------------------------------------------------     
    def start_thread(self ):
        self.stop_thread()
        self.cweb=cls_web()
        self.processed=0
        self.t1=threading.Thread(target=self._cli ,args='')
        self.t1.daemon=False
        self.start_time=time.time()
        self.end_time=time.time()
        self.t1.start()
                
#---------------------------------------------------------------------------------------------------------------------------------     
    def stop_thread(self):
        self.end_time=time.time()
        try:
            self.t1.join()
        except:
            pass
        #self.t1=None
            
#---------------------------------------------------------------------------------------------------------------------------------     
    def _cli(self ):
        if self.processed==0:
            for i in range(0 ,len(self.links)):
                #print("i={}  --  link={}  --  cnt={}".format(i ,self.links[i][0] ,self.links[i][1]) )
                self.links[i][1]=self.cweb.get_group_memcount_web( url=self.links[i][0] ,pxy=self.proxy )
            self.processed=1
            #links=None
        else:
            print("------------ not processed -------------------, thread name ={}".format(self.tname))
            
        self.stop_thread()
        return

################################################################################################################
class cls_web():
    def __init__(self ):
        pass

    def __del__(self):
        pass

#---------------------------------------------------------------------------------------------------------------------------------     
    def get_group_memcount_web(self ,url ,key="tgme_page_extra" ,pxy={}):
        u = url.strip()
        key=key.lower()
        #if True:
        try:
            #print("\n\n\n\n  --- with proxy --\n{}\n\n\n\n".format(pxy))
            if len(pxy)>0:
                code = requests.get(u ,proxies=pxy)
            else:
                code = requests.get(u)
            #print(url)
            #print(u)
            #print(code.text)
        #try:
            pass
        except:
            winsound.Beep(700,100)
            #winsound.Beep(1500,400)
            #winsound.Beep(1500,500)
            #print("\nURL can not opened.\n",u)
            return -404
        plain = code.text
        sp  = BeautifulSoup(plain, "html.parser")
        lst = sp.find_all(class_ = key)
        cnt=-2
        #[<div class="tgme_page_extra">30 899 members, 527 online</div>]
        #<a class="tgme_action_button_new" href="tg://join?invite=OUCwjUirJrJWWt2lAYzMkg">Join Group</a>
        #print(lst)
        button_key="tgme_action_button_new"
        for k in lst:
            itm=str(k)
            itm=itm.lower().strip()
            a=itm.find("member")
            b=itm.find(">")
            if len(itm)>0 and a>-1 and b>-1:
                ts=itm[b+1:a].replace(",","").replace(" ","").strip()
                if ts.isnumeric():
                    btlst = sp.find_all(class_ = button_key)
                    for u in btlst:
                        if str(u).lower().strip().find("join group")>-1:
                            cnt=int(ts)
                    return cnt
        return cnt

################################################################################################################
class cls_db():
    def __init__(self ,dbpath ,gcnn=False):
        self.db_path=dbpath.strip()
        self.global_cnn_flag=gcnn
        self.g_cnn = None
        self.g_cur = None
        
        self.all_allowed_chars="abcdefghijklmnopqrstuvwxyz"
        self.all_allowed_chars+=self.all_allowed_chars.upper()
        self.all_allowed_chars+="_-0123456789"
        
        if self.global_cnn_flag:
            self.set_global_connection()

    def __del__(self):
        del self.g_cur
        del self.g_cnn
        pass
#---------------------------------------------------------------------------------------------------------------------------------     
    def set_global_connection(self ,pth=""):
        tp=pth.strip()
        if len(tp)<1:   tp=self.db_path.strip()
        self.g_cnn = sqlite3.connect(tp)
        self.g_cur = self.g_cnn.cursor()

#---------------------------------------------------------------------------------------------------------------------------------     
    def lab(self ,pth=""):
        tp=pth.strip()
        if len(tp)<1:   tp=self.db_path.strip()
        res="no"
        if True:
        #try:

            alldata=self.get_db_data()

            #if self.global_cnn_flag:
            #    conn = self.g_cnn
            #else:
            #    conn = sqlite3.connect(tp)
            conn = sqlite3.connect(tp)
            cur = conn.cursor()

            out=cur.execute("select * from links where link like 'https://t.me/joinchat/Lpz3zE9yi_89jvsTso_X8Q' ")
            #out=cur.execute("select * from links where link like 'https://t.me/joinchat/N9lTaUnxfssxeVvLmLiQlQ' ")

            
            
            
            #cur.execute('update links set memcount=-100 where link="https://t.me/joinchat/Lak3xVYaaQrdv_65_Sy0Sw"')
            #cur.execute('update links set memcount=-100 where link="https://t.me/joinchat/J6iIDFYaaQrw1m5Xjkp4aQ"')

            
            #cur.execute("delete from links where link like '%\"%' ")
            #out=cur.execute('select link ,count(link) as cnt from links  group by link having cnt>=2 order by link')
            #Https://t.me/joinchat/MWmmpky_0G9uOwQ20bPNvQ 
            #out=cur.execute('select * from links where link like "%telegram%" ')
            #out=cur.execute('select * from links ')
            #cur.execute("delete from links where link ='Ihthttps'")
            #cur.execute('update links set link="{}" ,memcount={} where link="{}"'.format("https://t.me/joinchat/Lpz3zE9yi_89jvsTso_X8Q" ,-1 ,"https://teleG.me/joinchat/Lpz3zE9yi_89jvsTso_X8Q"))
            #conn.commit()
            #return

        
            #with open("all-------.txt","w+") as f:
            cnt=1
            for r in out:
                #print("\n i=",cnt)
                print(len(r[0]))
                print(r[0] ,r[1])
                #f.write(r[0] + "\n")
                cnt+=1
                
            res = "OK"
        try:
            pass
        except:
            pass
        finally:
            conn.close()
            conn = None
            cur = None
        return res
    
#---------------------------------------------------------------------------------------------------------------------------------     
    def create_table(self ,pth=""):
        tp=pth.strip()
        if len(tp)<1:   tp=self.db_path.strip()
        res="no"
        try:
            conn = sqlite3.connect(tp)
            cur = conn.cursor()
            #ALTER TABLE [yourTable] DROP COLUMN ID 
            #ALTER TABLE [yourTable] ADD ID INT IDENTITY(1,1)
            cur.execute('CREATE TABLE links (link text(255) ,memcount int)')
            cur.execute(' CREATE INDEX "link-idx-01" ON "links" ( "link" ASC); ')
            conn.commit()
            
            res = "OK"
        except:
            pass
        finally:
            conn.close()
            conn = None
            cur = None
        return res

#---------------------------------------------------------------------------------------------------------------------------------             
    def delete_from_table(self ,pth=""):
        tp=pth.strip()
        if len(tp)<1:   tp=self.db_path.strip()
        res="no"
        try:
            conn = sqlite3.connect(tp)
            cur = conn.cursor()
            cur.execute('delete from links')
            conn.commit()
            res = "OK"
        except:
            pass
        finally:
            conn.close()
            conn = None
            cur = None
        return res

#---------------------------------------------------------------------------------------------------------------------------------             
    def update_one(self ,pth="" ,link="" ,cnt=-2):
        link=link.strip()
        tp=pth.strip()
        if len(tp)<1:   tp=self.db_path.strip()
        res="no"
        if link=="": return res

        if True:
        #try:
            conn = sqlite3.connect(tp)
            cur = conn.cursor()
            #cur.execute('update links set link="{}" ,memcount={} where link="{}"'.format(link ,cnt ,link))
            cur.execute('update links set memcount={} where link="{}"'.format(cnt ,link))
            conn.commit()
            res = "OK"
        try:
            pass
        except:
            pass
        finally:
            conn.close()
            conn = None
            cur = None
        return res

#---------------------------------------------------------------------------------------------------------------------------------             
    def update_from_list(self ,pth="" ,update_list=[]):
        tp=pth.strip()
        if len(tp)<1:   tp=self.db_path.strip()
        res="no"
        if len(update_list)<1: return res

        if True:
        #try:
            conn = sqlite3.connect(tp)
            cur = conn.cursor()

            #print(update_list)
            lst=[]
            for r in update_list:
                lst.append( [r[1],r[0]] )
            #print(lst)
            #time.sleep(30)
            cur.executemany('update links set memcount=? where link=?', lst)
            #print("updated {}/{}".format(i,len(update_list)))
            #cur.execute('update links set link="{}" ,memcount={} where link="{}"'.format(link ,cnt ,link))

            #print("-----------    wait 100s      ----------")
            #time.sleep(100)
            conn.commit()
            res = "OK"
        try:
            pass
        except:
            pass
        finally:
            conn.close()
            conn = None
            cur = None
        return res

#---------------------------------------------------------------------------------------------------------------------------------             
    def update_one_global_connection(self ,link="" ,cnt=-2):
        res="no"
        if link=="": return res

        if True:
        #try:
            self.g_cur.execute('update links set link="{}" ,memcount={} where link="{}"'.format(link ,cnt ,link))
            self.g_cnn.commit()
            res = "OK"
        try:
            pass
        except:
            pass
        finally:
            #cur = None
            pass
        return res

#---------------------------------------------------------------------------------------------------------------------------------     
    def get_link_heart(self ,txt ):
        ts= txt.strip()
        if ts.lower().find("/joinchat/")<0:
            return ""
        ts = ts.replace("\\" ,"/")
        while ts.find("//")>-1:
            ts=ts.replace("//","/")
        ts = ts.split("/joinchat/")[1].strip()
        #if ts.lower().find("http")>-1 or ts.lower().find("telegram")>-1 or ts.lower().find("t.me")>-1:
        #    return ""
        for i in range(0 ,len(ts)):
            if not ts[i] in self.all_allowed_chars:
                ts=ts[:i]
                break
        return ts.strip()
        
#---------------------------------------------------------------------------------------------------------------------------------     
    def telegram_2_t(self ,txt ):
        ts= txt.strip()
        if ts.lower().find("/joinchat/")<0:
            return ""
        sp = ts.split("/")
        for i in range(0 ,len(sp)):
            if sp[i].lower().find("http")>-1 or sp[i].lower().find("joinchat")>-1 \
                       or sp[i].lower().find("telegram.me")>-1 or sp[i].lower().find("t.me")>-1:
                sp[i]=sp[i].lower()
            if sp[i].lower().find("telegram.me")>-1 :
                sp[i]=sp[i].replace("telegram.me" ,"t.me")
            if sp[i].find("http")>-1 :
                sp[i]="https:/"

        ts = "/".join(sp[i])
        return ts
        
#---------------------------------------------------------------------------------------------------------------------------------     
    def find_link(self ,txt ):
        txt=txt.strip()
        ts=""
        if txt.lower().find("https")>-1 and txt.lower().find("/joinchat/")>-1:
            i=txt.lower().find("https")
            ts = txt[i:len(txt)].split("\n")[0]
            if ts.find("joinchat")<0:
                print("\n\n\n\n\n-------- error in find_link (joinchat not found) ------------")
                print("ts='" ,ts,"'")
        else:
            return ts
                
        must_remove_chars="<>\"'"
        for c in must_remove_chars:
            ts=ts.replace(c,"")

        t=self.get_link_heart(ts)
        if len(t)>8:
            t="https://t.me/joinchat/"+t
            return t.strip()
        
        return ""

#---------------------------------------------------------------------------------------------------------------------------------     
    def insert_table(self ,msg ,pth="" ): 
        tp=pth.strip()
        if len(tp)<1:   tp=self.db_path.strip()
        res="no"

        flg=0
        added_cnt=0
        if True :
        #try:
            conn = sqlite3.connect(tp)
            cur = conn.cursor()
            for i in range(0,len(msg)):
                #print(i+1,in_data[i])
                ts=msg[i].message.strip()
                #print ("------ts1------: ",ts)
                ts=self.find_link(ts).strip()
                #print(msg[i]["message"])
                #print ("------ts2------: ",ts)
                if len(ts)>0:
                    cnt=cur.execute("select count(*) from links where link='{}'".format(ts)).fetchone()
                    #print("ssssssssssssssssssssssss",cnt)
                    if cnt[0]<1:
                        cur.execute("insert into links (link,memcount ) values('{}' ,-1) ".format(ts))
                        added_cnt+=1
                        flg=-1
                if flg!=added_cnt and added_cnt%100==0 or (i+1)%10000==0:
                    flg=added_cnt
                    print("write to db :: {} -- added count=( {} )".format((i+1) ,added_cnt))
            conn.commit()
            print("write to db ::: {} -- added count=( {} )".format((i+1) ,added_cnt))
            res = "OK"
        try:
            pass
        except:
            pass
        finally:
            conn.close()
            conn = None
            cur = None
        return res

#---------------------------------------------------------------------------------------------------------------------------------     
    def insert_table_textfile(self ,txtfn ,pth="" ): 
        tp=pth.strip()
        if len(tp)<1:   tp=self.db_path.strip()
        res=["no",0]

        flg=0
        added_cnt=0
        tmp_data = {}
        if True :
        #try:
            s=""
            with open(txtfn ,"r") as f:
                s=f.read().strip()
            s=s.replace(">","\n").replace("<","\n")
            sp=s.split("\n")
            conn = sqlite3.connect(tp)
            cur = conn.cursor()

            cur.execute( 'select link from links ' )
            tmp_data={row[0] for row in cur.fetchall()}
            
            for i in range(0,len(sp)):
                #print(i+1,in_data[i])
                ts=sp[i].strip()
                #print ("------ts1------: ",ts)
                ts=self.find_link(ts).strip()
                #print(msg[i]["message"])
                #print ("------ts2------: ",ts)
                if len(ts)>0:

                    #       this code replaced with below code
                    #cnt=cur.execute("select count(*) from links where link='{}'".format(ts)).fetchone()
                    ##print("ssssssssssssssssssssssss",cnt)
                    #if cnt[0]<1:
                    #    cur.execute("insert into links (link,memcount ) values('{}' ,-1) ".format(ts))
                    #    added_cnt+=1
                    #    flg=-1
                    
                    if ts not in tmp_data:
                        cur.execute("insert into links (link,memcount ) values('{}' ,-1) ".format(ts))
                        tmp_data.add(ts)
                        added_cnt+=1
                        flg=-1
                        
                if flg!=added_cnt and added_cnt%100==0 or (i+1)%10000==0:
                    flg=added_cnt
                    print("write to db :: {} -- added count=( {} )".format((i+1) ,added_cnt))
            conn.commit()
            print("write to db ::: {} -- added count=( {} )".format((i+1) ,added_cnt))
            res = ["OK",added_cnt]
        try:
            pass
        except:
            pass
        finally:
            conn.close()
            conn = None
            cur = None
            del tmp_data
        return res

#---------------------------------------------------------------------------------------------------------------------------------     
    def print_db(self ,pth=""):
        tp=pth.strip()
        if len(tp)<1:   tp=self.db_path.strip()
        res="no"
        if True:
        #try:
            conn = sqlite3.connect(tp)
            cur = conn.cursor()
            #cur.execute("delete from links where link like '%\"%' ")
            #out=cur.execute('select link as lwr ,count(lower(link)) as cnt from links  group by lower(link) having cnt>=2 order by lower(link)')
            #out=cur.execute('select * from links where lower(link)=lower("https://teleGram.me/joinchat/O_r9NUmrcEcQ1ISHXXSykw") ')
            out=cur.execute('select * from links ')

            cnt=1
            for r in out:
                print("\n i=",cnt)
                print(r)
                cnt+=1
            res = "OK"
        try:
            pass
        except:
            pass
        finally:
            conn.close()
            conn = None
            cur = None
        return res
    
#---------------------------------------------------------------------------------------------------------------------------------     
    def save_2_csv(self ,dbpth="" ,min_member_cnt=10000 ,outfn="out.csv"):
        tp=dbpth.strip()
        if len(tp)<1:   tp=self.db_path.strip()
        res="no"
        if True:
        #try:
            conn = sqlite3.connect(tp)
            cur = conn.cursor()
            out=cur.execute('select * from links where memcount>={}'.format(min_member_cnt))
            #out=cur.execute('select * from links where memcount=-2')
            cnt=1
            with open(outfn ,"w+") as f:
                for r in out:
                    f.write(r[0] +"," + str(r[1]) + "\n")                    
                    cnt+=1
            res = "OK"
        try:
            pass
        except:
            pass
        finally:
            conn.close()
            conn = None
            cur = None
        return res

#---------------------------------------------------------------------------------------------------------------------------------     
    def get_db_data(self ,pth="" ,sql_select=" * " ,sql_where="" ,tbl="links"):
        tp=pth.strip()
        if len(tp)<1:   tp=self.db_path.strip()
        out=[]
        total_cnt=0
        try:
            conn = sqlite3.connect(tp)
            cur = conn.cursor()

            #if alldata[j][1]==-100:  continue       # manualy disabled          *******************************
            #if alldata[j][1]>0:  continue           # processed earlier
            #if alldata[j][1]==-2:  continue         # link xpired
            total_cnt=cur.execute('select count(*) from {} '.format(tbl) ).fetchone()[0]
            out=cur.execute('select {} from {} {}'.format(sql_select ,tbl ,sql_where) ).fetchall()
            #cur.execute('select {} from {} {}'.format(sql_select ,tbl ,sql_where) )
            #out=[row[0] for row in cur.fetchall()]
            #cnt=1
            #for r in out:
            #    print("\n\n i=",cnt)
            #    print(r)
            #    cnt+=1
        except:
            pass
        finally:
            conn.close()
            conn = None
            cur = None
        return [out ,total_cnt]

#---------------------------------------------------------------------------------------------------------------------------------
    def norm_path(self ,p):
        p=str(p).strip()
        if len(p)<1:        return p
        p=p.replace("\r","")
        p=p.replace("\n","")
        p=p.replace("\\","/")
        while p.find("//",0)>-1:
                p=p.replace("//","/")
        if p[-1:]!='/' and p.find(".")<0:     p=p+'/'
        #if p[:1]!='/':     p='/'+p
        if len(p)==1 and p=="/":     p=""
        return p.lower()
    
#---------------------------------------------------------------------------------------------------------------------------------
    def all_files(self ,p ,target):
        out=[]
        for root, dirs, files in os.walk(p):
            for file in files:
                if file.endswith(target):
                    out.append(self.norm_path(root+"/")+ file)
        return out
     
#################################################################################################################
#################################################################################################################
#################################################################################################################
#################################################################################################################
#################################################################################################################
#---------------------------------------------------------------------------------------------------------------------------------     
def merge_textfiles(pth ,outfn="__merged-out.txt"):
    db= cls_db("1")
    pth=db.norm_path(pth)
    outfn=db.norm_path(outfn)
    if outfn.find("/")<0:
        outfn=pth + outfn
    
    if os.path.isdir(pth):
        file_list=[]
        file_list=db.all_files(pth ,".txt")
        ot=""
        sr={""}
        for i in range(0, len(file_list)):
            print(i+1 , len(ot))
            #print(file_list[i])
            t=""
            with open(file_list[i],"r") as f:
                s=f.read()
                ot+=s
                t=s.split("\n")
            for row in t:
                s=row.strip()
                k=s.find("http")
                if k>0:
                    s=s[k:len(s)]
                sr.add(s)
        print(outfn)
        with open(outfn ,"w") as f:
            f.write(ot)
        with open(outfn+"--noDuplicate.txt" ,"w+") as f:
            f.write("\n".join(sr))
        print("merged.")
        return "OK"
    
    print("not merged..")
    return "no"

#---------------------------------------------------------------------------------------------------------------------------------     
def get_link_from_textfile( dbfn="Persian-links.db" ,txtfn="RawText001.txt" ):
    db= cls_db(dbfn)
    res=db.create_table()
    if res=="OK":
        print("Table created seccessfully in db --> " ,dbfn)
    #db.delete_from_table()


    file_list=[]
    if os.path.isdir(txtfn):
        file_list=db.all_files(txtfn ,".txt")
        file_list+=db.all_files(txtfn ,".html")
    elif os.path.isfile(txtfn):
        file_list=[txtfn]
    else:
        print("wrong file-path\nexiting..")
        return
        
    #os.path.splitext("J:/123.123/456.123")    
    total_cnt=0
    for i in range(0,len(file_list)):
        winsound.Beep(2000,40)
        print("\n\ninsert_table_text/html-file({})  ( {}/{} )".format(file_list[i] ,i+1 ,len(file_list)))
        res=db.insert_table_textfile(txtfn=file_list[i])
        total_cnt+=res[1]
        print("res={}".format(res[0]))
    
    print("------------------------- added count( {} ) -------------------------".format(total_cnt))
    #print("===============  print DB  ===================")
    #db.print_db()

    del db
    #del cli
    winsound.Beep(2000,70)
    winsound.Beep(2000,500)
    
#---------------------------------------------------------------------------------------------------------------------------------     
def get_member_count( dbfn="Persian-links.db" ,outfn="" ,min_member_cnt=10000 ):
    db= cls_db(dbfn)
    res=db.create_table()
    if res=="OK":
        print("Table created seccessfully in db --> " ,dbfn)
    wb=cls_web()
    outfn=outfn.strip()
    if len(outfn)<1 :   outfn=os.path.splitext(dbfn)[0]+"_out.csv"
        
    #db.print_db()
    #db.lab()
    #res=db.save_2_csv(min_member_cnt=1 ,outfn=outfn)
    #return

    fnd_cnt=0
    healthy_link=0
    tm1=time.time()
    tm2=0
    processed_cnt=0

    #res=db.get_db_data()
    res = db.get_db_data( sql_where=" where memcount<1 and memcount<>-100 and memcount<>-2" )
    #res = db.get_db_data( sql_where=" where memcount=-2" )
    alldata=res[0]
    print("Total record count = {}  --  will process = ( {} )".format(res[1] ,len(alldata)) )

    for i in range(0,len(alldata)):
        if alldata[i][1]==-100:  continue       # manualy disabled          *******************************
        
        if alldata[i][1]>0:  continue           # processed earlier
        if alldata[i][1]==-2:  continue         # link xpired
        tm2=tm1
        cnt=wb.get_group_memcount_web( url=alldata[i][0] )
        processed_cnt+=1
        res = db.update_one(link=alldata[i][0] ,cnt=cnt)
        if cnt >= min_member_cnt :   fnd_cnt+=1
        if cnt > 0:                 healthy_link+=1
        tm1=time.time()
        diff_tm=(tm1-tm2)
        hh = (diff_tm * (len(alldata)-i-1)) / 3600
        mm = (hh-int(hh))*60
        ss = (mm-int(mm))*60
        total_percent = healthy_link/processed_cnt *100
        print( "\nupdate '{}' \nmember count=                        {} \nupdate_one -- res={}   processed {} / {} -({} - {})".format(alldata[i][0] ,cnt ,res ,i+1 ,len(alldata) ,len(alldata)-i-1 ,processed_cnt ) )
        print( "founded : {} - valid_link : {} ({}%)   remains={}:{}:{} -- {} s".format(fnd_cnt ,healthy_link ,"%.1f" % total_percent ,int(hh) ,int(mm) ,int(ss) ,"%.3f" % diff_tm) )
        #time.sleep(0.01)

    res=db.save_2_csv(min_member_cnt=min_member_cnt ,outfn=outfn)
    print("\n\n write to CSV is : {}".format(res))
    #db.print_db()

    del db
    del wb
    #del cli

#---------------------------------------------------------------------------------------------------------------------------------     
def check_proxy( in_proxyfn="Proxies.txt" ,out_proxyfn="Proxies-Valid.txt" ,url="https://t.me/joinchat/GjuWAEsaFwM-v58LJ3UPNA"):
    wb=cls_web()
    #t_url="https://t.me/durov"

    proxy=[]
    with open(in_proxyfn ,"r") as f:
        s=f.read().strip()
    if len(s)>0:
        sp=s.split("\n")
        print("\n loaded from {} -- proxy count = {}".format(in_proxyfn,len(sp)))
        if len(sp)>0:
            for i in range(0,len(sp)):
                sp[i]=sp[i].strip()
            i=0
            j=len(sp)
            while i<len(sp):
                tpxy =  {
                            'http'  : "http://"+sp[i],
                            'https' : "http://"+sp[i],
                        }
                print("\n\nremains : {}\n{}".format(j,tpxy))
                cnt=wb.get_group_memcount_web( url=url ,pxy=tpxy )
                print("index( {} ) : value returned = {}".format(i,cnt))
                if cnt<1:
                    print("count less than 1  --  proxy( {} ) not respond, deleted ..".format(i))
                    sp.pop(i)
                else:
                    i+=1
                j-=1
                
    with open(out_proxyfn ,"w") as f:
        s="\n".join(sp)
        f.write(s.strip())

    print("\n".join(sp))
    print("\n writed to {} -- proxy count = {}".format(out_proxyfn,len(sp)))
    return

#---------------------------------------------------------------------------------------------------------------------------------     
def load_from_file_2_list(ul_fn):
    lst=[]
    with open(ul_fn ,"r") as f:
        s=f.read()
    sp=s.split("\n")

    for i in range(0,len(sp)):
        t=sp[i].strip().split(",")
        if len(t)>1:
            item=[t[0].strip(),int(t[1].strip())]
            #print(i,item)
            lst.append(item)
    
    return lst

#---------------------------------------------------------------------------------------------------------------------------------     
def save_update_list_2_file(ul_fn ,lst):
    res="no"
    s=""
    for i in range(0,len(lst)):
        s+=str(lst[i][0])+","+str(lst[i][1])+"\n"
    with open(ul_fn ,"w") as f:
        f.write(s)
    res="OK"

#---------------------------------------------------------------------------------------------------------------------------------     
def get_member_count_thread( dbfn="Persian-links.db" ,outfn="" ,min_member_cnt=10000 ,proxy_fn="" ):
    #cli = [cls_client(phone, api_id, api_hash)]
    db= cls_db(dbfn)
    res=db.create_table()
    if res=="OK":
        print("Table created seccessfully in db --> " ,dbfn)
    wb=cls_web()
    outfn=outfn.strip()
    if len(outfn)<1 :   outfn=os.path.splitext(dbfn)[0]+"_out.csv"
        
    #db.print_db()
    #db.lab()
    #res=db.save_2_csv(min_member_cnt=min_member_cnt ,outfn=outfn)
    #res=db.save_2_csv(min_member_cnt=0 ,outfn=outfn)
    #return

    proxy=[]
    if os.path.isfile(proxy_fn):
        with open(proxy_fn ,"r") as f:
            s=f.read().strip()
        if len(s)>0:
            sp=s.split("\n")
            if len(sp)>0:
                for i in range(0,len(sp)):
                    sp[i]=sp[i].strip()
                    if len(sp[i])>0:
                        tpxy =  {
                                    'http'  : "http://"+sp[i],
                                    'https' : "http://"+sp[i],
                                }
                        if tpxy not in proxy:
                            proxy.append(  tpxy )
        print("\n\nProxies :  count({})".format(len(proxy)))
        for i in range(0,len(proxy)):
            print(proxy[i])
        print("Proxies :  count({})".format(len(proxy)))
        time.sleep(2)

    fnd_cnt=0
    healthy_link=0
    processed_cnt=0
    main_sleep_time = 0.2
    trd_sleep_time = 0.271
    threads_cnt = 25#len(proxy)*14 #400
    threads = []
    print_step=1
    max_update_list_len = 100
    last_valid_link_limit1 = 170
    last_valid_link_limit2 = 210
    last_valid_link_limit3 = 250
    update_list_fn = "update-list-out.txt"

    if False:#True:#False:#True:#False:#True:#False:#True:
        tlist=load_from_file_2_list(update_list_fn)
        ttt1=time.time()
        print("\n\nloading update list from file ")
        res = db.update_from_list( update_list=tlist )
        print("updating from list: was : {}\nupdate time : {}".format(res,time.time()-ttt1))
        for yy in range(200 ,1000 ,100): winsound.Beep(yy,int(yy/5))
        del tlist
        return

    #res = db.get_db_data( sql_where=" where rowid>471090 and rowid%50=0 and memcount<1 and memcount<>-100 and memcount<>-2" )
    res = db.get_db_data( sql_where=" where memcount<1 and memcount<>-100 and memcount<>-2" )
    #res = db.get_db_data( sql_where=" where memcount=-2" )
    alldata=res[0]
    total_record_count = res[1]
    print("Total record count = {}  --  will process = ( {} )".format(total_record_count ,len(alldata)) )

    for i in range(0,threads_cnt):
        k=int(len(alldata)/threads_cnt)
        start_idx=i*k
        end_idx=(i+1)*k
        if end_idx-start_idx>1:     end_idx-=1
        p={}
        if len(proxy)>0:
            rnd_idx=random.randrange(0,len(proxy))
            print("random index for proxies : {}".format(rnd_idx))
            p=proxy[rnd_idx] 
        threads.append([cls_thread(thread_name=str(i),pxy=p) ,start_idx ,end_idx,end_idx-start_idx+1])

    threads[len(threads)-1][2] = len(alldata)-1
    threads[len(threads)-1][3] = threads[len(threads)-1][2] - threads[len(threads)-1][1] +1
        
    for i in range(0,threads_cnt):
        print(threads[i][0],threads[i][1],threads[i][2] )

    #db_g= cls_db(dbfn ,gcnn=True)

    total_cnt_remain = total_record_count
    last_valid_link=-1
    to_update=[]
    while len(alldata)>0:
        alive_threads=0
        #cls_wnd.root.update()

        for i in range(0,threads_cnt):
            if threads[i][0].processed!=0:
                if threads[i][0].processed==1:
                    j=threads[i][1]

                    #change proxy here
                    
                    lnk=threads[i][0].links[0][0]
                    cnt=threads[i][0].links[0][1]
                    processed_cnt+=1
                    res="--"

                    #   ---- update ----
                    to_update.append([lnk,cnt])
                    #time.sleep(0.5)
                    if len(to_update)>max_update_list_len:
                        ttt1=time.time()
                        print("\n\nupdating data -- len = {}".format(len(to_update)))
                        res = db.update_from_list( update_list=to_update )
                        print("\n\nupdating data done -- time = {}".format(time.time()-ttt1))
                        to_update=[]
                    #res = db.update_one(link=lnk ,cnt=cnt)
                    #res = db_g.update_one_global_connection(link=lnk ,cnt=cnt)
                    #   ---- update ----
                    
                    if cnt >= min_member_cnt :      fnd_cnt+=1
                    if cnt > 0:
                        healthy_link+=1
                        last_valid_link = processed_cnt

                    if last_valid_link>-1:
                        if processed_cnt - last_valid_link >last_valid_link_limit3:
                            for yy in range(200 ,1000 ,100): winsound.Beep(yy,int(yy/5))
                            winsound.Beep(4000,850)
                            winsound.Beep(4000,850)
                            winsound.Beep(4000,850)
                        elif processed_cnt - last_valid_link > last_valid_link_limit2:
                            winsound.Beep(300,100)
                            winsound.Beep(300,500)
                        elif processed_cnt - last_valid_link > last_valid_link_limit1:
                            winsound.Beep(150,50)

                    az_idx = threads[i][1]
                    ta_idx = threads[i][2]
                    trd_totalcnt = threads[i][3]
                    trd_remain_cnt=ta_idx-az_idx+1
                    trd_processed=trd_totalcnt-trd_remain_cnt
                    tm2 = threads[i][0].start_time
                    tm1 = threads[i][0].end_time
                    diff_tm=(tm1-tm2)
                    hh = (diff_tm * trd_remain_cnt) / 3600
                    mm = (hh-int(hh))*60
                    ss = (mm-int(mm))*60

                    total_remain_time_sec =trd_remain_cnt * (diff_tm + main_sleep_time + trd_sleep_time*threads_cnt+0.000)   # 0.25 for update db
                    total_hh = (total_remain_time_sec) / 3600
                    total_mm = (total_hh-int(total_hh))*60
                    total_ss = (total_mm-int(total_mm))*60

                    total_percent = healthy_link/processed_cnt *100

                    if i % print_step==0:
                        print( "\nThread name = '{}'      mem-cnt=         <<  {}  >>\nupdate '{}' -- res={}\ndone {}/{}-({}-{}) -thread-remains~={}:{}:{} -- {}s".format(threads[i][0].tname ,cnt ,alldata[j][0],res ,az_idx+1 ,ta_idx+1 ,trd_remain_cnt ,trd_processed ,int(hh) ,int(mm) ,int(ss) ,"%.3f" % diff_tm ) )
                        print( "total founded : {} - total valid_link :      **{}**      ({}%)\nTotalRemainTime ~={}:{}:{}s - TotalProccessed= {}  /  ({})".format(fnd_cnt ,healthy_link ,"%.1f" % total_percent ,int(total_hh) ,int(total_mm) ,int(total_ss),processed_cnt ,total_cnt_remain) )
                    
                    #time.sleep(0.01)
                    threads[i][1]+=1

                if threads[i][1]<=threads[i][2]:
                    j=threads[i][1]

                    #if alldata[j][1]==-100:  continue       # manualy disabled          *******************************
                    #if alldata[j][1]>0:  continue           # processed earlier
                    #if alldata[j][1]==-2:  continue         # link xpired

                    threads[i][0].links=[]
                    threads[i][0].links.append(    [ alldata[j][0],alldata[j][1] ]    )
                    threads[i][0].start_thread()
                    print("{} - sleep {}s after start thread - from-last-valid   **{}".format(i ,trd_sleep_time ,processed_cnt - last_valid_link))
                    if trd_sleep_time>0:
                        time.sleep(trd_sleep_time)
            else:
                pass    #is processing

        #if len(to_update)>max_update_list_len:
        #    ttt1=time.time()
        #    print("\nsaving update list to file: -- count( {} )".format(len(to_update)))
        #    save_update_list_2_file(update_list_fn ,to_update)
        #    print("wrote to text file (bkp) -- result was {}\nupdate time : {}".format(res ,time.time()-ttt1))
        #    #print("\nupdating from list: -- count( {} )".format(len(to_update)))
        #    #res = db.update_from_list( update_list=to_update )
        #    #print("updating from list: result was : {}\nupdate time : {}".format(res,time.time()-ttt1))
        #    #to_update=[]
        #    max_update_list_len +=500
            
        total_cnt_remain=0
        for i in range(0,threads_cnt):
            #if threads[i].t1==None or threads[i].t1.is_alive()==False:
            total_cnt_remain += threads[i][2] - threads[i][1] + 1
            if threads[i][0].t1.is_alive()==True:
                alive_threads+=1

        #print_step+=1
        #if print_step>15:       print_step=5
        
        print("\n------------------------------------------Alive threads count = {}\nTotal remain count = ( {} )".format(alive_threads ,total_cnt_remain) )
        if total_cnt_remain<1: break
        #if alive_threads==0: break

        print("sleep {} s".format(main_sleep_time))
        if main_sleep_time>0:
            time.sleep(main_sleep_time)
        # end while --------------------------
        # end while --------------------------
        # end while --------------------------

    if len(to_update)>0:
        ttt1=time.time()
        print("\n\nupdating last part -- len = {}".format(len(to_update)))
        res = db.update_from_list( update_list=to_update )
        print("\n\nupdating data done -- time = {}".format(time.time()-ttt1))
        to_update=[]

            #print("\nupdating from list: -- count( {} )".format(len(to_update)))
            #res = db.update_from_list( update_list=to_update )
            #print("updating from list: result was : {}\nupdate time : {}".format(res,time.time()-ttt1))
            #to_update=[]

    #if True:
    #    tlist=load_from_file_2_list(update_list_fn)
    #    ttt1=time.time()
    #    print("\n\nloading update list from file ")
    #    res = db.update_from_list( update_list=tlist )
    #    print("updating from list: was : {}\nupdate time : {}".format(res,time.time()-ttt1))
    #    del tlist

    # db class with global connection must terminated befor save_2_csv
    #del db_g

    res=db.save_2_csv(min_member_cnt=min_member_cnt ,outfn=outfn)
    print("\n\n write to CSV is : {}".format(res))
    #db.print_db()

    for i in range(0,threads_cnt):
        threads[i][0].stop_thread()
        del threads[i][0]
    del threads
    del to_update
    del db
    del wb
    #del cli

    return

#################################################################################################################
#################################################################################################################

my_dbfn="Telegram-links.db"


#   load links from text file
#get_link_from_textfile( dbfn=my_dbfn ,txtfn="C:/textfiles/" )
#get_link_from_textfile( dbfn=my_dbfn ,txtfn="bg_links_out.txt" )


#   check proxies           --- we need an active link to che proxies
#check_proxy( in_proxyfn="Proxies-Valid-B-00.txt" ,out_proxyfn="Proxies-Valid-B-01.txt" ,url="https://t.me/joinchat/GjuWAEsaFwM-v58LJ3UPNA")


#   link checker
#get_member_count( dbfn=my_dbfn ,min_member_cnt=10000 )


#   link checker    -- with thread
#get_member_count_thread ( dbfn=my_dbfn ,min_member_cnt=0 ,proxy_fn="Proxies-Valid-B-01.txt")
get_member_count_thread ( dbfn=my_dbfn ,min_member_cnt=0 )


#   merg files
#from="D:/banklink/data2/"
#to="D:/banklink/All-in-one/All-in-one.txt"
#merge_textfiles(from ,to)
