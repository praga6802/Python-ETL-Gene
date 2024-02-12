import pymysql as sql
f=open(r"C:\Users\praga\OneDrive\Documents\Besant\Python\project\gene.txt","r",encoding='utf-8')
content=f.readlines()
f.close()
#print(content)
gene_name=content[0].split(" ")[0]
gene_id=content[1].split(":")[1].split(",")[0].strip()
#print(gene_name)
#print(gene_id)
connection=sql.connect(user="root",port=3306,host="localhost",password="Praga@060802")
db=connection.cursor()
def get_data(data_content):
    result={}
    ex_no=1
    for i in data_content:
        e_dt=i.split("\t")[0]
        e_st=int(e_dt.split("-")[0])
        e_ed=int(e_dt.split("-")[1])
        e_sz=e_ed - e_st if e_ed > e_st else e_st - e_ed
        result[ex_no]={"st":e_st,"ed":e_ed,"sz":e_sz}
        ex_no+=1
    return result

def create_database(db_name):
    db.execute("create database if not exists %s"%(db_name))
   
def create_table(db_name,tb_name):
    table="create table if not exists %s(gene_name varchar(250),gene_id varchar(250),trans_id varchar(250),ex varchar(250),ex_st varchar(250),ex_ed varchar(250),ex_sz varchar(250))"%(tb_name)
    db.execute(f"use {db_name}")
    db.execute(table)

def insert_data(db_name,tb_name,gene_name,gene_id,trans_id,ex,ex_st,ex_ed,ex_sz):
    db.execute("use %s"%(db_name))
    insert=(f"insert into {tb_name}(gene_name,gene_id,trans_id,ex,ex_st,ex_ed,ex_sz) values ('{gene_name}','{gene_id}','{trans_id}','{ex}','{ex_st}','{ex_ed}','{ex_sz}')")
    db.execute(insert)
    connection.commit()

if __name__=="__main__":
    create_database("Gene")
    create_table("Gene","Gene_table")
    db_name="Gene"
    tb_name="gene_table"
    with open("gene.csv","w",encoding='utf-8')as f:
            f.write("Gene name,Gene ID,Trans ID,Ex no,Ex start,Ex End,Ex Size")
            index=[i for i in range(len(content)) if "mRNA transcript" in content[i]]
            #print(index)
            for i in range(len(index)):
                if i !=len(index)-1:
                    st,ed=index[i],index[i+1]
                    #print(st,ed)
                    data_content=content[st:ed]
                    #print(data_content)
                    tr_id=[i for i in data_content[0].split(" ")if "NM_" in i][0]
                    tr_id=tr_id.replace(",","")
                    #print(tr_id)
                    hypen=[i for i in range(len(data_content)) if "--------"in data_content[i]][0]
                    #print(hypen)
                    data_content=data_content[hypen+1:]
                    data_content=[i.strip() for i in data_content if i.strip()!='']
                    fd=get_data(data_content)
                    for ex in fd:
                        f.write("\n %s,%s,%s,%s,%s,%s,%s"%(gene_name,gene_id,tr_id,ex,fd[ex]["st"],fd[ex]["ed"],fd[ex]["sz"]))
                        insert_data(db_name,tb_name,gene_name,gene_id,tr_id,ex,fd[ex]["st"],fd[ex]["ed"],fd[ex]["sz"])
                    #print(data_content)
                    #print("\n")
                else:
                    st=index[i]
                    #print(st)
                    data_content=content[st:]
                    #print(data_content)
                    tr_id=[i for i in data_content[0].split(" ")if "NM_" in i][0]
                    tr_id=tr_id.replace(",","")
                    #print(tr_id)
                    hypen=[i for i in range(len(data_content)) if "--------"in data_content[i]][0]
                    #print(hypen)
                    data_content=data_content[hypen+1:]
                    data_content=[i.strip()for i in data_content if i.strip()!='']
                    fd=get_data(data_content)
                    for ex in fd:
                        f.write("\n %s,%s,%s,%s,%s,%s,%s"%(gene_name,gene_id,tr_id,ex,fd[ex]["st"],fd[ex]["ed"],fd[ex]["sz"]))
                        insert_data(db_name,tb_name,gene_name,gene_id,tr_id,ex,fd[ex]["st"],fd[ex]["ed"],fd[ex]["sz"])
                    #print(data_content)
                    #print("\n")

db.close()
connection.close()
print("done")