from database import Database
db=Database('inherit2_test',True,False)

'''
db.create_table('FLNames',['surname','lname'],[str,str])
db.create_table('Address',['address_name','number'],[str,int])
db.create_table('Telephones',['phone_number'],[int],None,['FLNames','Address'])
db.create_table('Married_Status',['Married'],[bool],None,['Telephones'])
db.insert('Married_Status',['Xrhstos','Rimpas','Ksenofwntos',100,698765454,True])
db.insert('Married_Status',['Isidwros','Tsalapatis','PindouDavaki',50,693565454,False])
db.insert('Married_Status',['Nafsika','Mastrodhma','Papandreou',300,693568554,False])
db.insert('Married_Status',['Panos','Vagiannhs','Petroupolews',56,690519599,True])
db.insert('Married_Status',['Eua','Pakou','Solwnos',53,690519500,False])
db.insert('Married_Status',['Panagiwths','Triantafullou','Ypsilantoy',19,693568500,True])
db.insert('Married_Status',['Makhs','Papathimiopoulos','Agiou Nikolaou',45,690568500,True])
db.insert('Married_Status',['Dhmhtrhs','Papadopoulos','Sofokleous',345,694561093,False])

db.update('Telephones',"Nikolopoulou","address_name","surname == Isidwros")

db.show_table('Married_Status')
db.show_table('Telephones')
db.show_table('Address')
db.show_table('FLNames')


db.create_table('Students',['Name','Lname','Age','Address_Name','Address_Number','Married'],[str,str,int,str,int,bool])
db.partition('Students','Age')
db.create_partition('PrimaryEducation_Students','Students',17)
db.create_partition('College_Students','Students',18)
db.create_partition('Senior_Students','Students',22)

db.insert('Students',['Xrhstos','Rimpas',17,'Ksenofwntos',100,True])
db.insert('Students',['Isidwros','Tsalapatis',18,'PindouDavaki',50,False])
db.insert('Students',['Nafsika','Mastrodhma',17,'Papandreou',300,False])
db.insert('Students',['Panos','Vagiannhs',18,'Petroupolews',56,True])
db.insert('Students',['Eua','Pakou',22,'Solwnos',53,False])
db.insert('Students',['Panagiwths','Triantafullou',22,'Ypsilantoy',19,True])
db.insert('Students',['Makhs','Papathimiopoulos',18,'Agiou Nikolaou',45,True])
db.insert('Students',['Dhmhtrhs','Papadopoulos',22,'Sofokleous',345,False])
db.update('Students',False,'Married',"Age == 17")
'''

db.delete('Students',"Age > 17")



db.show_table('Students')
db.show_table('PrimaryEducation_Students')
db.show_table('College_Students')
db.show_table('Senior_Students')
