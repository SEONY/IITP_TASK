import sys

if len(sys.argv) < 2:
    print "need DB name for gen"
    exit(1)

new_db = sys.argv[1]

# Make funcs.sh
funcs_file = open("./gen_source/funcs.sh",'r')
funcs_lines = funcs_file.readlines()

with open("./gen_source/gen_funcs.sh", 'w') as gen_file:
    for index, line in enumerate(funcs_lines):
        gen_file.write(line)
        splited_line = line.split()
        if len(splited_line) >= 2 and splited_line[1] == "AGJ1":
            gen_file.write("\t" + new_db + ")\n");
            gen_file.write("\t    cp=\"../lib/" + new_db + "/*:../lib/*\"\n");
            gen_file.write("\t    ;;\n")
        if len(splited_line) >= 2 and splited_line[1] == "AGJ2":
            gen_file.write("    " + new_db + "|")

funcs_file.close()
print "generate funcs.sh success !"

# Make jTPCC.java
jtpcc_file = open("./gen_source/jTPCC.java", 'r')
jtpcc_lines = jtpcc_file.readlines()

with open("./gen_source/gen_jTPCC.java", 'w') as gen_file:
    for index, line in enumerate(jtpcc_lines):
        gen_file.write(line)
        splited_line = line.split()
        
        if len(splited_line) >= 2 and splited_line[1] == "AGJ":
            gen_file.write("\telse if (iDB.equals(\"" + new_db + "\"))\n")
            gen_file.write("\t    dbType = DB_" + new_db.upper() + ";\n")
        

jtpcc_file.close()
print "generate jTPCC.java success !"

# Make JTPCCConfig.java
jconfig_file = open("./gen_source/jTPCCConfig.java", 'r')
jconfig_lines = jconfig_file.readlines()

with open("./gen_source/gen_jTPCCConfig.java", 'w') as gen_file:
    for index, line in enumerate(jconfig_lines):
        gen_file.write(line)
        splited_line = line.split()
        if len(splited_line) >= 2 and splited_line[1] == "AGJ":
            gen_file.write("\t\t\t\tDB_" + new_db.upper() + " = 4,\n")

jconfig_file.close()
print "generate jTPCCConfig.java success !"
