import WSERDataImport;


def main():  
    availablity_files = ["C:/Users/koeniga/Documents/Files_and_Code/WSER Pull 1.xlsx",
                         "C:/Users/koeniga/Documents/Files_and_Code/WSER1.xlsx",
                         "C:/Users/koeniga/Documents/Files_and_Code/WSER-Pull 1.xlsx" 
                        ]
    amrep_defects_files = ["C:/Users/koeniga/Documents/Files_and_Code/Aircraft Delivered Quality Stoplight (Jun 15 Data).xlsx"]
    
    db_connection = WSERDataImport.TWSERDatabaseConnection(password="wseruser")
    for fx in availablity_files:                           
       df_avail = WSERDataImport.TAvailablityData(fx) 
       db_connection.AddNewAvailablityData(df_avail)          
    for fx in amrep_defects_files: 
         amrep_data = WSERDataImport.AmrepTableData(fx) 
         db_connection.AddNewAmrepData(amrep_data)                            
         defects_data = WSERDataImport.DefectsTableData(fx) 
         db_connection.AddNewDefectsData(defects_data)
    db_connection.Close()
    
if __name__ == "__main__":
    main()
