CREATE TABLE IDL (
	IDL_id SERIAL PRIMARY KEY,
	IDL_Value FLOAT(6),
	Concentration_pg FLOAT(6)
);

CREATE TABLE GC_Method (
	GC_Method_id TEXT PRIMARY KEY,
	SplitRatio SMALLINT,
	Chromatrography VARCHAR(2),
	RunTime_min FLOAT(2)
);

CREATE TABLE MS_Method (
	MS_Method_id TEXT PRIMARY KEY,
	AcquisitionRate SMALLINT,
	MassRange_Bottom SMALLINT,
	MassRange_Top SMALLINT,
	ExtractionFrequency FLOAT(2),
	DetectorOffset_Volts SMALLINT
);

CREATE TABLE DataSet (
	DataSet_id TEXT PRIMARY KEY,
	Instrument VARCHAR(3),
	IDL_id INTEGER REFERENCES IDL(IDL_id),
	GC_Method_id TEXT REFERENCES GC_Method(GC_Method_id),
	MS_Method_id TEXT REFERENCES MS_Method(MS_Method_id)
);

CREATE TABLE IonStats (
	IonStats_id SERIAL PRIMARY KEY,
	Voltage FLOAT(1),
	AreaPerIon FLOAT(6)
);

CREATE TABLE Sample (
	Sample_id SERIAL PRIMARY KEY,
	DateTimeStamp TIMESTAMP,
	SampleType TEXT,
	SampleName TEXT,
	IonStats_id INTEGER REFERENCES IonStats(IonStats_id),
	DataSet_id TEXT REFERENCES DataSet(DataSet_id)
);

CREATE TABLE PeakTable (
	PeakTable_id SERIAL PRIMARY KEY,
	Analyte TEXT,
	DataProcessingType TEXT,
	Area BIGINT,
	Height BIGINT,
	FWHH_S FLOAT(6),
	Similairity SMALLINT,
	RT_1D FLOAT(3),
	RT_2D FLOAT(3),
	PEAK_SN INTEGER,
	QUANT_SN INTEGER,
	Concentration_pg FLOAT(6),
	Sample_id INTEGER REFERENCES Sample(Sample_id)
);