# Databricks notebook source
import pyspark.sql.functions as F

pathemplotyee = "/mnt/fo/raw/employees"
pathdepartment = "/mnt/fo/raw/departments"
pathdesignation = "/mnt/fo/raw/designations"
pathroletype = "/mnt/fo/raw/crew_role_types"
pathoperation = "/mnt/fo/raw/operations" 

catalog = "analyticadebuddha.flightoperation"

employeecols = [
    "Id", F.col("employee_id").alias("EmployeeId"), F.col("operation_id").alias("OperationId"), F.col("department_id").alias("DepartmentId"), 
    F.col("designation_id").alias("DesignationId"), F.concat(F.col("f_name"), F.lit(" "), F.col("m_name"), F.lit(" "), F.col("l_name")).alias("FullName"), 
    F.col("Gender"), F.col("DOb"), F.col("Email"), F.col("mobile").alias("MobileNumber"), F.col("is_active").alias("IsActive"), 
    F.col("contractual_date").alias("ContractualDate"), F.col("created_at").alias("CreatedAt"), F.col("updated_at").alias("UpdateddAt")
]

departmentcol = [
    "Id", F.col("Name").alias("DepartmentName"), F.col("parent_id").alias("ParentDepartmentId"), "Code", "Status", F.col("job_status").alias("JobStatus"), 
    F.col("created_at").alias("CreatedAt"), F.col("updated_at").alias("UpdateddAt")
]

designationcols = [
    "Id", F.col("operation_id").alias("OperationId"), F.col("name").alias("DesignationName"), F.col("Grade"), 
    F.col("Level"), F.col("Code"), F.col("band_id").alias("BandId"), F.col("Order"), F.col("Status"), 
    F.col("created_at").alias("CreatedAt"), F.col("updated_at").alias("UpdateddAt")
]

employeeinfocol = [
    F.col("id").alias("EmployeeIdMain"),"EmployeeId", "FullName", "Gender", "DOb", "Email", "MobileNumber", "IsActive", "ContractualDate"
]

employothercol = [F.col("id").alias("EmployeeIdMain"),"EmployeeId", "OperationId", "DepartmentId", "DesignationId", "CreatedAt", "UpdateddAt"]

roletypecol = ["Id", "Name"]

def getDepartmetnInfo(department):
    '''
    '''
    parentdepartment = department.filter(F.col("ParentDepartmentId").isNotNull())
    parentdepartment_ = department.filter(F.col("ParentDepartmentId").isNull()).withColumns(
        {
            "Depid": F.col("id"),
            "DepartmentSub": F.col("DepartmentName")
        }
    )

    test = department.alias("df1").join(
        parentdepartment.alias("df2"), F.col("df1.Id") == F.col("df2.ParentDepartmentId"), how="inner"
    ).select("df1.*", F.col("df2.DepartmentName").alias("DepartmentSub"), F.col("df2.id").alias("Depid"))

    departments = parentdepartment_.unionByName(test, allowMissingColumns=True)

    return departments

def getemployeeInfo(employee):
    employeeinfo = employee.select(*employeeinfocol)
    employeeother = employee.select(*employothercol)

    employeeinfo.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.employeeinfo")

    return employeeother

def getStructuralInfo(employeeother, departments, designation, operations):
    '''
    Return the info related to the employee department and designation and the operations.
    '''
    structuralinfo1 = employeeother.alias("df1").join(
        departments.alias("df2"), (F.col("df1.DepartmentId") == F.col("df2.Depid")) & (F.col("df2.status") != "0"), how= "left"
    ).select(
        "df1.*", F.col("df2.DepartmentName"), F.col("df2.DepartmentSub"), F.col("df2.Code").alias("DepartmentCode"), 
        F.col("df2.status").alias("DepartmentStatus")
    )

    structuralinfo1 = structuralinfo1.alias("df1").join(
        designation.alias("df2"), (F.col("df1.DesignationId") == F.col("df2.Id")) & (F.col("df2.status") != "0"), how= "left"
    ).select(
        "df1.*", "df2.DesignationName", "df2.Grade", F.col("df2.Code").alias("PositionCode"), 
        F.col("df2.BandId"), F.col("df2.Order"), F.col("df2.Status").alias("DesiginationStatus")
    )

    structuralinfo = structuralinfo1.alias("df1").join(
        operations.alias("df2"), F.col("df1.OperationId") == F.col("df2.id"), how= "left"
    ).select("df1.*", F.col("df2.name").alias("OperationName")).drop("CreatedAt", "UpdateddAt")

    structuralinfo.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.department_designation")

    return None

def main():
    '''
    '''
    employee = spark.read.format("delta").load(pathemplotyee).filter(F.col("deleted_at").isNull()).select(*employeecols)
    department  = spark.read.format("delta").load(pathdepartment).filter(F.col("deleted_at").isNull()).select(*departmentcol)
    designation = spark.read.format("delta").load(pathdesignation).select(*designationcols)
    spark.read.format("delta").load(pathroletype).select(*roletypecol).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.roletype")
    operations = spark.read.format("delta").load(pathoperation)

    departments = getDepartmetnInfo(department)
    employeeother = getemployeeInfo(employee)
    getStructuralInfo(employeeother, departments, designation, operations)

    return None

if __name__ == "__main__":
    main()