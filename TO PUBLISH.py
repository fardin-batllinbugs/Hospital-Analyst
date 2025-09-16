def load_data():
    USER = ""  # your username
    PASSWORD = ""  # your password
    HOST = ""  # server host
    PORT = ""  # default PostgreSQL port
    DBNAME = ""  # your database name
    engine = create_engine(f"+://{}:{}@{}:{}/{}")
    # Example connection (Postgres)
    #engine = create_engine("postgresql://user:password@localhost:5432/hospital_db")

    bills = pd.read_sql("SELECT * FROM bills", engine)
    patients = pd.read_sql("SELECT * FROM patients", engine)
    staff = pd.read_sql("SELECT * FROM staff", engine)
    departments = pd.read_sql("SELECT * FROM departments", engine)
    appointments = pd.read_sql("SELECT * FROM appointments", engine)
    beds = pd.read_sql("SELECT * FROM beds", engine)

    print("✔ Data loaded successfully")
    print(f" Bills: {len(bills)} rows")
    print(f" Patients: {len(patients)} rows")
    print(f" Staff: {len(staff)} rows")
    print(f" Departments: {len(departments)} rows")
    print(f" Appointments: {len(appointments)} rows")
    print(f" Beds: {len(beds)} rows")

    return bills, patients, staff, departments, appointments, beds


# -------------------------------
# STEP 2: DATA VALIDATION / QA
# -------------------------------
def run_validation(bills, patients, staff, departments):
    print("\n==== DATA VALIDATION ====")

    # 2.1 Missing values
    print("\n🔎 Missing values (critical columns):")
    print(bills[["bill_id", "patient_id", "total_amount", "amount_paid"]].isnull().sum())

    # 2.2 Duplicate bills
    duplicates = bills[bills.duplicated(
        subset=["patient_id", "payment_date", "total_amount"], keep=False)]
    print("\n🔎 Duplicate bills:")
    print(duplicates if not duplicates.empty else "None found ✅")

    # 2.3 Overpaid bills
    invalid_paid = bills[bills["amount_paid"] > bills["total_amount"]]
    print("\n🔎 Overpaid bills:")
    print(invalid_paid if not invalid_paid.empty else "None found ✅")

    # 2.4 Mismatched patient IDs
    invalid_patient_ids = bills[~bills["patient_id"].isin(patients["patient_id"])]
    print("\n🔎 Bills with invalid patient IDs:")
    print(invalid_patient_ids if not invalid_patient_ids.empty else "None found ✅")

    # 2.5 Discharged patients without bills
    discharged_patients = patients[patients["discharge_date"].notna()]
    no_bill_patients = discharged_patients[~discharged_patients["patient_id"].isin(bills["patient_id"])]
    print("\n🔎 Discharged patients without bills:")
    print(no_bill_patients if not no_bill_patients.empty else "None found ✅")

    # 2.6 Payment status distribution
    print("\n🔎 Payment status distribution (%):")
    print(bills["payment_status"].value_counts(normalize=True) * 100)

    # 2.7 Revenue totals check
    bill_sum = bills["total_amount"].sum()
    dept_revenue_sum = bills.groupby("department_id")["total_amount"].sum().sum()
    print("\n🔎 Revenue totals check:")
    print(f" From all bills: {bill_sum}")
    print(f" From dept sums: {dept_revenue_sum}")


# -------------------------------
# STEP 3: BUSINESS ANALYSIS
# -------------------------------
def run_analysis(bills, patients, staff, departments, appointments, beds):
    print("\n==== BUSINESS ANALYSIS ====")

    # Revenue per month
    bills["payment_date"] = pd.to_datetime(bills["payment_date"])
    revenue_month = bills.groupby(bills["payment_date"].dt.to_period("M"))["total_amount"].sum().reset_index(name="monthly_revenue")
    print("\n📊 Monthly Revenue:")
    print(revenue_month)

    # Patients per month
    patients["admission_date"] = pd.to_datetime(patients["admission_date"])
    admit_month = patients.groupby(patients["admission_date"].dt.to_period("M"))["patient_id"].count().reset_index(name="admitted_patients")
    print("\n📊 Patients Admitted per Month:")
    print(admit_month)

    # Bed occupancy %
    beds["is_occupied"] = beds["patient_id"].notna()
    bed_occ = beds.groupby("department_id")["is_occupied"].mean().reset_index()
    bed_occ["occupancy_pct"] = (bed_occ["is_occupied"] * 100).round(2)

    # Doctor revenue ranking
    appoint_bill = pd.merge(bills, appointments, on="appointment_id", how="left")
    appoint_staff = pd.merge(appoint_bill, staff[["staff_id", "name", "department_id"]], on="staff_id", how="left")
    appoint_staff = pd.merge(appoint_staff, departments[["department_id", "department_name"]], on="department_id", how="left")

    doctor_rank = appoint_staff.groupby(["department_name", "name"])["total_amount"].sum().reset_index(name="revenue")
    doctor_rank["rank_in_dept"] = doctor_rank.groupby("department_name")["revenue"].rank(method="dense", ascending=False)
    print("\n👨‍⚕️ Doctor Ranking per Department:")
    print(doctor_rank)

    # Active patients
    active_patients = patients[patients["discharge_date"].isna()]
    print("\n🏥 Active Patients (still admitted):")
    print(active_patients)

    # Avg stay days
    patients["discharge_date"] = pd.to_datetime(patients["discharge_date"], errors="coerce")
    patients["stay_days"] = (patients["discharge_date"] - patients["admission_date"]).dt.days
    avg_stay = patients.groupby("department_id")["stay_days"].mean().reset_index(name="avg_stay")

    # Revenue last 6 months
    cutoff_date = bills["payment_date"].max() - DateOffset(months=6)
    recent_bills = bills[bills["payment_date"] >= cutoff_date]
    dept_revenue_6m = recent_bills.groupby("department_id")["total_amount"].sum().reset_index(name="revenue_last_6m")
    print("\n💰 Revenue per Department (last 6 months):")
    print(dept_revenue_6m)

    # Department summary
    dept_revenue = bills.groupby("department_id")["total_amount"].sum().reset_index(name="total_revenue")
    dept_summary = dept_revenue.merge(bed_occ[["department_id", "occupancy_pct"]], on="department_id", how="left")
    dept_summary = dept_summary.merge(avg_stay, on="department_id", how="left")
    dept_summary = pd.merge(dept_summary, departments[["department_id", "department_name"]], on="department_id", how="left")

    print("\n🏥 Department Summary:")
    print(dept_summary)


# -------------------------------
# MAIN EXECUTION
# -------------------------------
if __name__ == "__main__":
    bills, patients, staff, departments, appointments, beds = load_data()

    # Run validation first
    run_validation(bills, patients, staff, departments)

    # Run analysis
    run_analysis(bills, patients, staff, departments, appointments, beds)
