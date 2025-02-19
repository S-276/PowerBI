-- ==========================================
-- STAR SCHEMA AND SUN MODEL (Future-Ready)
-- ==========================================

-- 1. Create Database
CREATE DATABASE WebLogDW;
USE WebLogDW;

-- 2. Create Date Dimension
CREATE TABLE DateDim (
    DateID INT IDENTITY PRIMARY KEY,
    FullDate DATE NOT NULL,
    Day INT NOT NULL,
    Month INT NOT NULL,
    Quarter INT NOT NULL,
    Year INT NOT NULL
);

-- 3. Create Client Dimension
CREATE TABLE ClientDim (
    ClientID INT IDENTITY PRIMARY KEY,
    IPAddress VARCHAR(50) NOT NULL,
    City VARCHAR(50),
    Country VARCHAR(50),
    Browser VARCHAR(50),
    OS VARCHAR(50)
);

-- 4. Create Request Dimension
CREATE TABLE RequestDim (
    RequestID INT IDENTITY PRIMARY KEY,
    FileType VARCHAR(20) NOT NULL,
    URL VARCHAR(500) NOT NULL
);

-- 5. Create Error Dimension
CREATE TABLE ErrorDim (
    ErrorID INT IDENTITY PRIMARY KEY,
    HTTPStatusCode INT NOT NULL,
    ErrorType VARCHAR(100)
);

-- 6. Create Referrer Dimension
CREATE TABLE ReferrerDim (
    ReferrerID INT IDENTITY PRIMARY KEY,
    ReferrerURL VARCHAR(500)
);

-- ==========================================
-- FUTURE SCALABILITY ENHANCEMENTS
-- ==========================================

-- 7. Partitioning: Fact Table by Date
CREATE PARTITION FUNCTION RequestDateRange (DATE)
AS RANGE LEFT FOR VALUES ('2023-01-01', '2023-06-01', '2023-12-31');

CREATE PARTITION SCHEME RequestDateScheme
AS PARTITION RequestDateRange
ALL TO ([PRIMARY]);

-- 8. Recreate Fact Table with Partitioning
DROP TABLE IF EXISTS WebRequests;
CREATE TABLE WebRequests (
    RequestID INT IDENTITY,
    DateID INT NOT NULL,
    ClientID INT NOT NULL,
    RequestDimID INT NOT NULL,
    ErrorID INT NOT NULL,
    ReferrerID INT NOT NULL,
    BytesServed BIGINT,
    ProcessingTime INT,
    RequestDate DATE NOT NULL,
    CONSTRAINT PK_WebRequests PRIMARY KEY (RequestID, RequestDate),
    INDEX idx_RequestDate NONCLUSTERED (RequestDate),
    FOREIGN KEY (DateID) REFERENCES DateDim(DateID),
    FOREIGN KEY (ClientID) REFERENCES ClientDim(ClientID),
    FOREIGN KEY (RequestDimID) REFERENCES RequestDim(RequestID),
    FOREIGN KEY (ErrorID) REFERENCES ErrorDim(ErrorID),
    FOREIGN KEY (ReferrerID) REFERENCES ReferrerDim(ReferrerID)
) ON RequestDateScheme (RequestDate);

-- 9. Indexing for Big Data Performance
CREATE NONCLUSTERED INDEX idx_ClientID ON WebRequests (ClientID);
CREATE NONCLUSTERED INDEX idx_ProcessingTime ON WebRequests (ProcessingTime);
CREATE NONCLUSTERED INDEX idx_ReferrerID ON WebRequests (ReferrerID);


