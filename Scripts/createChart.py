# -*- coding: utf-8 -*-
"""
Created on Tue Oct 27 10:02:52 2015

@author: koeniga
"""
import gviz_api
import PosgresData
import webbrowser

def main(): 
    
    #data headers for gviz datatable
    description = {"ID": ("string", "ID"), "UPNR": ("number", "UPNR"), "NMCB": ("number", "NMCB"), "NMCS": ("number", "NMCS"), "NMCM": ("number", "NMCM"), "DEPOT": ("number", "DEPOT"),"AVAILABLE": ("number", "AVAILABLE") }
    
    #connect to posgreSQL database
    db_connection = PosgresData.TWSERDatabaseConnection(password="wseruser")
    
    md_id = []
    #loop if MDID is not found
    while len(md_id) < 1:
        #prompt user to enter an MD
        mdName = input("Enter a MD Name: ")
        
        
        #look up mdid of entered MD name
        md_id = db_connection.SelectMDIDs(mdName.upper())
        
        if len(md_id) == 0:
            print("Invalid MDID")

    #get all time data
    times = db_connection.GetTimeTable()
    for ii, row in times.iterrows():
        time_ids = times['ID']
        years = times['FYYear']
        quarters = times['FYQuarter']
    
    #create array that will be used to label collumns
    year_and_quarter = []
    for idx in range(len(time_ids)):
        year_and_quarter.append("Year " + str(years[idx]) + " Quarter " + str(quarters[idx]))
    
    
    #returns all MDTIMEs with each corresponding MD if MDID is valid
    if md_id:
        result = db_connection.SelectMDTIMEIDs(md_id[0][0])
        newResult = []
        #convert results from tuples to ints
        for y in result:
            strings = (''.join(map(str, y)))
            newResult.append(int(strings))
    
        #create list to store all data in 
        UPNR_array = []
        NMCB_array = []
        NMCS_array = []
        NMCM_array = []
        DEPOT_array = []
        AVAILABLE_array = []
        
        #if the md_id has values in the availability table, create chart for md_id
        if newResult:
            data = []
            time_index = []
            for q in newResult:
                
                #create list to hold values of time_ids
                time_id = db_connection.SelectTIMEIDs(q)
                time_index.append(time_id[0][0]-1)
                
                #get all availability data
                UPNR = db_connection.SelectUPNR(q)
                NMCB = db_connection.SelectNMCB(q)
                NMCS = db_connection.SelectNMCS(q)
                NMCM = db_connection.SelectNMCM(q)
                DEPOT= db_connection.SelectDEPOT(q)
                AVAILABLE = db_connection.SelectAVAILABLE(q)
                
                #the above methods return tuples inside of lists so accessed using [0][0]
                UPNR_array.append((UPNR[0][0] * 100))
                NMCS_array.append((NMCS[0][0] * 100))
                NMCB_array.append((NMCB[0][0] * 100))
                NMCM_array.append((NMCM[0][0] * 100))
                DEPOT_array.append((DEPOT[0][0] * 100))
                AVAILABLE_array.append((AVAILABLE[0][0] * 100))
    
            #create and add dicionary of values to data list
            #time_index[idx] contains location of the year and quarter array to place as column label
            for idx in range(len(UPNR_array)):
                data.append({"ID": year_and_quarter[time_index[idx]], "UPNR": UPNR_array[idx], "NMCB": NMCB_array[idx], "NMCS": NMCS_array[idx], "NMCM": NMCM_array[idx], "DEPOT": DEPOT_array[idx], "AVAILABLE": AVAILABLE_array[idx]})
            
            #temporary just to show 4 quarters
            while(len(data) < 4):
                data.append({"ID": "Year N/A Quarter N/A", "UPNR": 0, "NMCB": 0, "NMCS": 0, "NMCM": 0, "DEPOT": 0, "AVAILABLE": 0})
            
            #sort collumns of chart based on ID
            sortedData = sorted(data, key=lambda k: k['ID'])
            
            #start creating all the google viz tables and jscode
            data_table = gviz_api.DataTable(description)
            data_table.LoadData(sortedData)
            
            #create new javascript code to fill data of HTML file
            jscode = data_table.ToJSCode("jscode_data", columns_order=("ID", "AVAILABLE", "DEPOT", "UPNR", "NMCB", "NMCS", "NMCM"), order_by="ID")
            
            mdid = mdName.upper()
            
            #read template HTML file
            template_file = open('C:/Users/koeniga/Documents/GoogleVizCharts/googlevizChartTemplate.html', 'r')
            page_template = template_file.read()
            template_file.close()
            
            #open and write to output HTML file
            output_file = open('C:/Users/koeniga/Documents/GoogleVizCharts/googlevizChart.html', 'w')
            output_file.write(page_template % vars())
            output_file.close()
            
            #open new output file in web browser
            webbrowser.open_new_tab('C:/Users/koeniga/Documents/GoogleVizCharts/googlevizChart.html')
            
        else:
            print("No Chart Available For This MDID")
  
    else:
        print("Invalid MDID")
        
if __name__ == "__main__":
    main()
