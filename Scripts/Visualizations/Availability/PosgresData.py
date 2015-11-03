# -*- coding: utf-8 -*-
"""
Created on Thu Oct 15 09:44:26 2015

@author: koeniga
"""

import pandas as pd
import psycopg2 as pg2

#contains methods to access all data in posgres database needed for availability table

class TWSERDatabaseConnection:
    def __init__(self, host_name="localhost", port_number=5432, user_name="wseruser", password=None, db_name="WSERData"):
        self.conn = pg2.connect(database =db_name, host=host_name, port=port_number,
                                user=user_name, password=password)
    def GetMDTable(self):
        curr = self.conn.cursor()
        curr.execute("SELECT id, name FROM MDTbl;")
        results = curr.fetchall()
        df = pd.DataFrame(results, columns = ["ID", "Name"])
        curr.close()
        return df
        
    
    def GetTimeTable(self):
        curr = self.conn.cursor()
        curr.execute("SELECT id, fyyear, fyquarter FROM TimeTbl;")
        results = curr.fetchall()
        df = pd.DataFrame(results, columns = ["ID", "FYYear", "FYQuarter"])
        curr.close()
        return df        

    def GetMDTimeTable(self):
        curr = self.conn.cursor()
        curr.execute("SELECT id, md_id, time_id FROM MDTimeTbl;")
        results = curr.fetchall()
        df = pd.DataFrame(results, columns = ["ID", "MD_ID", "Time_ID"])
        curr.close()
        return df
        
    def GetAvailablityStandardsTable(self):
        curr = self.conn.cursor()
        curr.execute("SELECT MD_ID, FYear, UPNR, NMCB, NMCS, NMCM, DEPOT, AVAILABLE FROM availablitystandardstbl;")
        results = curr.fetchall()
        df = pd.DataFrame(results, columns = ["MD_ID", "FYear", "UPNR", "NMCB", "NMCS", "NMCM", "DEPOT", "AVAILABLE"])
        curr.close()
        return(df)
        
    def GetAvailablityTbl(self):
        curr = self.conn.cursor()
        curr.execute("SELECT MD_TIME_ID, UPNR, NMCB, NMCS, NMCM, DEPOT, AVAILABLE FROM AvailablityTbl;")
        results = curr.fetchall()
        df = pd.DataFrame(results, columns = ["MD_TIME_ID", "UPNR", "NMCB", "NMCS", "NMCM", "DEPOT", "AVAILABLE"])
        curr.close()
        return(df)
        
    def GetAMREPTable(self):
        curr = self.conn.cursor()
        curr.execute("SELECT id, md_id, usaf_designation, repair_org_type, completion_date FROM amreptable;")
        results = curr.fetchall()
        df = pd.DataFrame(results, columns = ["ID", "MD_ID", "USAF_DESIGNATION", "REPAIR_ORG_TYPE", "COMPLETION_DATE"])
        curr.close()
        return df
        
    
    def GetDEFECTSTable(self):
        curr = self.conn.cursor()
        curr.execute("SELECT id, md_id, usaf_designation, repair_org_type FROM defectstable;")
        results = curr.fetchall()
        df = pd.DataFrame(results, columns = ["ID", "MD_ID", "USAF_DESIGNATION", "REPAIR_ORG_TYPE"])
        curr.close()
        return df  
        
    def SelectMDTIMEIDs(self, md_id):
        curr = self.conn.cursor()
        curr.execute("SELECT id FROM MDtimeTbl WHERE md_id=%d;" %md_id)
        results = curr.fetchall()
        curr.close();
        return results
        
    def SelectMDIDs(self, name):
        curr = self.conn.cursor()
        curr.execute("SELECT id FROM MDTbl WHERE name=%r;" %name)
        results = curr.fetchall()
        curr.close();
        return results
        
    def SelectTIMEIDs(self, md_id):
        curr = self.conn.cursor()
        curr.execute("SELECT time_id FROM MDtimeTbl WHERE id=%d;" %md_id)
        results = curr.fetchall()
        curr.close();
        return results
        
    def SelectAvailabilitys(self, md_time_id):
        curr = self.conn.cursor()
        curr.execute("SELECT UPNR, NMCB, NMCS, NMCM, DEPOT, AVAILABLE FROM AvailablityTbl WHERE md_time_id=%d;" %md_time_id)
        results = curr.fetchall()
        curr.close();
        return results
        
    def SelectUPNR(self, md_time_id):
        curr = self.conn.cursor()
        curr.execute("SELECT UPNR FROM AvailablityTbl WHERE md_time_id=%d;" %md_time_id)
        results = curr.fetchall()
        curr.close();
        return results
    
    def SelectNMCB(self, md_time_id):
        curr = self.conn.cursor()
        curr.execute("SELECT NMCB FROM AvailablityTbl WHERE md_time_id=%d;" %md_time_id)
        results = curr.fetchall()
        curr.close();
        return results
        
    def SelectNMCS(self, md_time_id):
        curr = self.conn.cursor()
        curr.execute("SELECT NMCS FROM AvailablityTbl WHERE md_time_id=%d;" %md_time_id)
        results = curr.fetchall()
        curr.close();
        return results
        
    def SelectNMCM(self, md_time_id):
        curr = self.conn.cursor()
        curr.execute("SELECT NMCM FROM AvailablityTbl WHERE md_time_id=%d;" %md_time_id)
        results = curr.fetchall()
        curr.close();
        return results
        
    def SelectDEPOT(self, md_time_id):
        curr = self.conn.cursor()
        curr.execute("SELECT DEPOT FROM AvailablityTbl WHERE md_time_id=%d;" %md_time_id)
        results = curr.fetchall()
        curr.close();
        return results
        
    def SelectAVAILABLE(self, md_time_id):
        curr = self.conn.cursor()
        curr.execute("SELECT AVAILABLE FROM AvailablityTbl WHERE md_time_id=%d;" %md_time_id)
        results = curr.fetchall()
        curr.close();
        return results
        
    def Close(self):
        self.conn.close()
        
