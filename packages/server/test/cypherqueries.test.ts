import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { GraphDatabase } from "../src/db";
import { Executor, ExecutionResult } from "../src/executor";

/**
 * Tests for Cypher query patterns from /tmp/cypherqueries.json
 * These tests verify the graph database can handle all query patterns
 * from the CC (invoice/accounting) application.
 */
describe("CypherQueries.json Patterns", () => {
  let db: GraphDatabase;
  let executor: Executor;

  beforeEach(() => {
    db = new GraphDatabase(":memory:");
    db.initialize();
    executor = new Executor(db);
  });

  afterEach(() => {
    db.close();
  });

  function exec(cypher: string, params: Record<string, unknown> = {}): ExecutionResult {
    const result = executor.execute(cypher, params);
    if (!result.success) {
      throw new Error(`Query failed: ${result.error.message}`);
    }
    return result;
  }

  describe("User Operations", () => {
    it("creates a user and returns it", () => {
      // CREATE (u:CC_User {...}) RETURN u
      const result = exec(
        `CREATE (u:CC_User {
          id: $id,
          email: $email,
          passwordHash: $passwordHash,
          createdAt: $createdAt
        }) RETURN u`,
        {
          id: "user-1",
          email: "test@example.com",
          passwordHash: "hash123",
          createdAt: "2024-01-01",
        }
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].u).toBeDefined();
      const user = result.data[0].u as Record<string, unknown>;
      expect((user.properties as Record<string, unknown>).email).toBe("test@example.com");
    });

    it("finds a user by email", () => {
      // First create a user
      exec(
        `CREATE (u:CC_User {id: $id, email: $email, passwordHash: $passwordHash, createdAt: $createdAt})`,
        { id: "user-1", email: "alice@example.com", passwordHash: "hash", createdAt: "2024-01-01" }
      );

      // MATCH (u:CC_User {email: $email}) RETURN u
      const result = exec(`MATCH (u:CC_User {email: $email}) RETURN u`, {
        email: "alice@example.com",
      });

      expect(result.data).toHaveLength(1);
      const user = result.data[0].u as Record<string, unknown>;
      expect((user.properties as Record<string, unknown>).email).toBe("alice@example.com");
    });

    it("finds a user by id", () => {
      exec(
        `CREATE (u:CC_User {id: $id, email: $email, passwordHash: $passwordHash, createdAt: $createdAt})`,
        { id: "user-123", email: "bob@example.com", passwordHash: "hash", createdAt: "2024-01-01" }
      );

      // MATCH (u:CC_User {id: $id}) RETURN u
      const result = exec(`MATCH (u:CC_User {id: $id}) RETURN u`, { id: "user-123" });

      expect(result.data).toHaveLength(1);
      const user = result.data[0].u as Record<string, unknown>;
      expect((user.properties as Record<string, unknown>).id).toBe("user-123");
    });
  });

  describe("Business Operations", () => {
    it("gets business by user id through OWNS relationship", () => {
      // Create user
      exec(`CREATE (u:CC_User {id: $id, email: $email})`, {
        id: "user-1",
        email: "owner@example.com",
      });

      // Create business linked to user
      exec(
        `MATCH (u:CC_User {id: $userId})
         CREATE (u)-[:OWNS]->(b:CC_Business {
           id: $businessId,
           name: $name,
           address: $address
         })`,
        { userId: "user-1", businessId: "biz-1", name: "Acme Inc", address: "123 Main St" }
      );

      // MATCH (u:CC_User {id: $userId})-[:OWNS]->(b:CC_Business) RETURN b
      const result = exec(
        `MATCH (u:CC_User {id: $userId})-[:OWNS]->(b:CC_Business) RETURN b`,
        { userId: "user-1" }
      );

      expect(result.data).toHaveLength(1);
      const biz = result.data[0].b as Record<string, unknown>;
      expect((biz.properties as Record<string, unknown>).name).toBe("Acme Inc");
    });

    it("updates business properties and returns updated business", () => {
      // Setup
      exec(`CREATE (u:CC_User {id: $id, email: $email})`, {
        id: "user-1",
        email: "owner@example.com",
      });
      exec(
        `MATCH (u:CC_User {id: $userId})
         CREATE (u)-[:OWNS]->(b:CC_Business {id: $businessId, name: $name, address: $address})`,
        { userId: "user-1", businessId: "biz-1", name: "Old Name", address: "Old Address" }
      );

      // MATCH (u:CC_User {id: $userId})-[:OWNS]->(b:CC_Business)
      // SET b.name = $name, b.address = $address
      // RETURN b
      exec(
        `MATCH (u:CC_User {id: $userId})-[:OWNS]->(b:CC_Business)
         SET b.name = $name, b.address = $address`,
        { userId: "user-1", name: "New Name", address: "New Address" }
      );

      const result = exec(
        `MATCH (u:CC_User {id: $userId})-[:OWNS]->(b:CC_Business) RETURN b`,
        { userId: "user-1" }
      );

      const biz = result.data[0].b as Record<string, unknown>;
      expect((biz.properties as Record<string, unknown>).name).toBe("New Name");
      expect((biz.properties as Record<string, unknown>).address).toBe("New Address");
    });
  });

  describe("Customer Operations", () => {
    it("creates customer linked to user via HAS_CUSTOMER relationship", () => {
      exec(`CREATE (u:CC_User {id: $id, email: $email})`, {
        id: "user-1",
        email: "owner@example.com",
      });

      // Create customer
      exec(
        `MATCH (u:CC_User {id: $userId})
         CREATE (u)-[:HAS_CUSTOMER]->(c:CC_Customer {
           id: $customerId,
           name: $name,
           email: $email,
           vatRate: $vatRate
         })`,
        {
          userId: "user-1",
          customerId: "cust-1",
          name: "Customer Inc",
          email: "customer@example.com",
          vatRate: 21,
        }
      );

      const result = exec(
        `MATCH (u:CC_User {id: $userId})-[:HAS_CUSTOMER]->(c:CC_Customer) RETURN c`,
        { userId: "user-1" }
      );

      expect(result.data).toHaveLength(1);
      const customer = result.data[0].c as Record<string, unknown>;
      expect((customer.properties as Record<string, unknown>).name).toBe("Customer Inc");
    });

    it("gets customer with sequence through USES_SEQUENCE relationship", () => {
      // Setup user, customer, and sequence
      exec(`CREATE (u:CC_User {id: $id})`, { id: "user-1" });
      exec(
        `MATCH (u:CC_User {id: $userId})
         CREATE (u)-[:HAS_CUSTOMER]->(c:CC_Customer {id: $customerId, name: $name})`,
        { userId: "user-1", customerId: "cust-1", name: "Customer" }
      );
      exec(
        `MATCH (u:CC_User {id: $userId})
         CREATE (u)-[:HAS_SEQUENCE]->(s:CC_InvoiceSequence {id: $sequenceId, prefix: $prefix, lastNumber: $lastNumber})`,
        { userId: "user-1", sequenceId: "seq-1", prefix: "INV", lastNumber: 0 }
      );

      // Link customer to sequence
      exec(
        `MATCH (c:CC_Customer {id: $customerId})
         MATCH (s:CC_InvoiceSequence {id: $sequenceId})
         CREATE (c)-[:USES_SEQUENCE]->(s)`,
        { customerId: "cust-1", sequenceId: "seq-1" }
      );

      // MATCH (c:CC_Customer {id: $customerId})-[:USES_SEQUENCE]->(s:CC_InvoiceSequence)
      // RETURN s.id as sequenceId
      const result = exec(
        `MATCH (c:CC_Customer {id: $customerId})-[:USES_SEQUENCE]->(s:CC_InvoiceSequence)
         RETURN s.id as sequenceId`,
        { customerId: "cust-1" }
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].sequenceId).toBe("seq-1");
    });

    it("deletes USES_SEQUENCE relationship", () => {
      // Setup
      exec(`CREATE (u:CC_User {id: $id})`, { id: "user-1" });
      exec(
        `MATCH (u:CC_User {id: $userId})
         CREATE (u)-[:HAS_CUSTOMER]->(c:CC_Customer {id: $customerId, name: $name})`,
        { userId: "user-1", customerId: "cust-1", name: "Customer" }
      );
      exec(
        `MATCH (u:CC_User {id: $userId})
         CREATE (u)-[:HAS_SEQUENCE]->(s:CC_InvoiceSequence {id: $sequenceId, prefix: $prefix})`,
        { userId: "user-1", sequenceId: "seq-1", prefix: "INV" }
      );
      exec(
        `MATCH (c:CC_Customer {id: $customerId})
         MATCH (s:CC_InvoiceSequence {id: $sequenceId})
         CREATE (c)-[:USES_SEQUENCE]->(s)`,
        { customerId: "cust-1", sequenceId: "seq-1" }
      );

      // MATCH (c:CC_Customer {id: $customerId})-[r:USES_SEQUENCE]->() DELETE r
      exec(
        `MATCH (c:CC_Customer {id: $customerId})-[r:USES_SEQUENCE]->()
         DELETE r`,
        { customerId: "cust-1" }
      );

      // Verify relationship is gone
      const result = exec(
        `MATCH (c:CC_Customer {id: $customerId})-[:USES_SEQUENCE]->(s)
         RETURN s`,
        { customerId: "cust-1" }
      );
      expect(result.data).toHaveLength(0);
    });

    it("archives and unarchives customer", () => {
      exec(`CREATE (u:CC_User {id: $id})`, { id: "user-1" });
      exec(
        `MATCH (u:CC_User {id: $userId})
         CREATE (u)-[:HAS_CUSTOMER]->(c:CC_Customer {id: $customerId, name: $name, archived: false})`,
        { userId: "user-1", customerId: "cust-1", name: "Customer" }
      );

      // Archive customer
      // MATCH (u:CC_User {id: $userId})-[:HAS_CUSTOMER]->(c:CC_Customer {id: $customerId})
      // SET c.archived = true
      exec(
        `MATCH (u:CC_User {id: $userId})-[:HAS_CUSTOMER]->(c:CC_Customer {id: $customerId})
         SET c.archived = true`,
        { userId: "user-1", customerId: "cust-1" }
      );

      let result = exec(
        `MATCH (u:CC_User {id: $userId})-[:HAS_CUSTOMER]->(c:CC_Customer {id: $customerId})
         RETURN c`,
        { userId: "user-1", customerId: "cust-1" }
      );
      let customer = result.data[0].c as Record<string, unknown>;
      expect((customer.properties as Record<string, unknown>).archived).toBe(true);

      // Unarchive customer
      exec(
        `MATCH (u:CC_User {id: $userId})-[:HAS_CUSTOMER]->(c:CC_Customer {id: $customerId})
         SET c.archived = false`,
        { userId: "user-1", customerId: "cust-1" }
      );

      result = exec(
        `MATCH (u:CC_User {id: $userId})-[:HAS_CUSTOMER]->(c:CC_Customer {id: $customerId})
         RETURN c`,
        { userId: "user-1", customerId: "cust-1" }
      );
      customer = result.data[0].c as Record<string, unknown>;
      expect((customer.properties as Record<string, unknown>).archived).toBe(false);
    });

    it("detach deletes customer", () => {
      exec(`CREATE (u:CC_User {id: $id})`, { id: "user-1" });
      exec(
        `MATCH (u:CC_User {id: $userId})
         CREATE (u)-[:HAS_CUSTOMER]->(c:CC_Customer {id: $customerId, name: $name})`,
        { userId: "user-1", customerId: "cust-1", name: "Customer" }
      );

      // MATCH (u:CC_User {id: $userId})-[:HAS_CUSTOMER]->(c:CC_Customer {id: $customerId})
      // DETACH DELETE c
      exec(
        `MATCH (u:CC_User {id: $userId})-[:HAS_CUSTOMER]->(c:CC_Customer {id: $customerId})
         DETACH DELETE c`,
        { userId: "user-1", customerId: "cust-1" }
      );

      const result = exec(
        `MATCH (c:CC_Customer {id: $customerId}) RETURN c`,
        { customerId: "cust-1" }
      );
      expect(result.data).toHaveLength(0);
    });

    it("counts invoices for customer", () => {
      // Setup
      exec(`CREATE (u:CC_User {id: $id})`, { id: "user-1" });
      exec(
        `MATCH (u:CC_User {id: $userId})
         CREATE (u)-[:HAS_CUSTOMER]->(c:CC_Customer {id: $customerId, name: $name})`,
        { userId: "user-1", customerId: "cust-1", name: "Customer" }
      );

      // Create invoices linked to user and customer
      for (let i = 1; i <= 3; i++) {
        exec(
          `MATCH (u:CC_User {id: $userId})
           MATCH (c:CC_Customer {id: $customerId})
           CREATE (u)-[:HAS_INVOICE]->(i:CC_Invoice {id: $invoiceId, invoiceNumber: $invoiceNumber})-[:BILLED_TO]->(c)`,
          { userId: "user-1", customerId: "cust-1", invoiceId: `inv-${i}`, invoiceNumber: `INV-${i}` }
        );
      }

      // MATCH (u:CC_User {id: $userId})-[:HAS_INVOICE]->(i:CC_Invoice)-[:BILLED_TO]->(c:CC_Customer {id: $customerId})
      // RETURN COUNT(i) as invoiceCount
      const result = exec(
        `MATCH (u:CC_User {id: $userId})-[:HAS_INVOICE]->(i:CC_Invoice)-[:BILLED_TO]->(c:CC_Customer {id: $customerId})
         RETURN COUNT(i) as invoiceCount`,
        { userId: "user-1", customerId: "cust-1" }
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].invoiceCount).toBe(3);
    });
  });

  describe("Report Operations", () => {
    it("gets reports by user id", () => {
      exec(`CREATE (u:CC_User {id: $id})`, { id: "user-1" });

      for (let month = 1; month <= 3; month++) {
        exec(
          `MATCH (u:CC_User {id: $userId})
           CREATE (u)-[:HAS_REPORT]->(r:CC_MonthlyReport {id: $reportId, year: $year, month: $month, status: $status})`,
          { userId: "user-1", reportId: `report-${month}`, year: 2024, month, status: "pending" }
        );
      }

      // MATCH (u:CC_User {id: $userId})-[:HAS_REPORT]->(r:CC_MonthlyReport) RETURN r
      const result = exec(
        `MATCH (u:CC_User {id: $userId})-[:HAS_REPORT]->(r:CC_MonthlyReport) RETURN r`,
        { userId: "user-1" }
      );

      expect(result.data).toHaveLength(3);
    });

    it("finds report by year and month in WHERE clause", () => {
      exec(`CREATE (u:CC_User {id: $id})`, { id: "user-1" });
      exec(
        `MATCH (u:CC_User {id: $userId})
         CREATE (u)-[:HAS_REPORT]->(r:CC_MonthlyReport {id: $reportId, year: $year, month: $month, status: $status})`,
        { userId: "user-1", reportId: "report-jan", year: 2024, month: 1, status: "pending" }
      );
      exec(
        `MATCH (u:CC_User {id: $userId})
         CREATE (u)-[:HAS_REPORT]->(r:CC_MonthlyReport {id: $reportId, year: $year, month: $month, status: $status})`,
        { userId: "user-1", reportId: "report-feb", year: 2024, month: 2, status: "complete" }
      );

      // MATCH (u:CC_User {id: $userId})-[:HAS_REPORT]->(r:CC_MonthlyReport)
      // WHERE r.year = $year AND r.month = $month
      // RETURN r
      const result = exec(
        `MATCH (u:CC_User {id: $userId})-[:HAS_REPORT]->(r:CC_MonthlyReport)
         WHERE r.year = $year AND r.month = $month
         RETURN r`,
        { userId: "user-1", year: 2024, month: 1 }
      );

      expect(result.data).toHaveLength(1);
      const report = result.data[0].r as Record<string, unknown>;
      expect((report.properties as Record<string, unknown>).id).toBe("report-jan");
    });

    it("updates report status", () => {
      exec(`CREATE (r:CC_MonthlyReport {id: $reportId, status: $status})`, {
        reportId: "report-1",
        status: "pending",
      });

      // MATCH (r:CC_MonthlyReport {id: $reportId}) SET r.status = $status
      exec(`MATCH (r:CC_MonthlyReport {id: $reportId}) SET r.status = $status`, {
        reportId: "report-1",
        status: "complete",
      });

      const result = exec(`MATCH (r:CC_MonthlyReport {id: $reportId}) RETURN r`, {
        reportId: "report-1",
      });
      const report = result.data[0].r as Record<string, unknown>;
      expect((report.properties as Record<string, unknown>).status).toBe("complete");
    });
  });

  describe("Bank Statement Operations", () => {
    it("creates bank statement linked to report", () => {
      exec(`CREATE (r:CC_MonthlyReport {id: $reportId, status: $status})`, {
        reportId: "report-1",
        status: "pending",
      });

      // MATCH (r:CC_MonthlyReport {id: $reportId})
      // CREATE (r)-[:HAS_BANK_STATEMENT]->(bs:CC_BankStatement {...})
      exec(
        `MATCH (r:CC_MonthlyReport {id: $reportId})
         CREATE (r)-[:HAS_BANK_STATEMENT]->(bs:CC_BankStatement {
           id: $statementId,
           bank: $bank,
           pdfPath: $pdfPath,
           uploadedAt: $uploadedAt
         })`,
        {
          reportId: "report-1",
          statementId: "stmt-1",
          bank: "ING",
          pdfPath: "/uploads/stmt.pdf",
          uploadedAt: "2024-01-15",
        }
      );

      // MATCH (r:CC_MonthlyReport {id: $reportId})-[:HAS_BANK_STATEMENT]->(bs:CC_BankStatement) RETURN bs
      const result = exec(
        `MATCH (r:CC_MonthlyReport {id: $reportId})-[:HAS_BANK_STATEMENT]->(bs:CC_BankStatement)
         RETURN bs`,
        { reportId: "report-1" }
      );

      expect(result.data).toHaveLength(1);
      const stmt = result.data[0].bs as Record<string, unknown>;
      expect((stmt.properties as Record<string, unknown>).bank).toBe("ING");
    });

    it("counts transactions in bank statement", () => {
      exec(`CREATE (bs:CC_BankStatement {id: $id, bank: $bank})`, {
        id: "stmt-1",
        bank: "ING",
      });

      for (let i = 1; i <= 5; i++) {
        exec(
          `MATCH (bs:CC_BankStatement {id: $statementId})
           CREATE (t:CC_Transaction {id: $txId, amount: $amount, status: $status})-[:PART_OF]->(bs)`,
          { statementId: "stmt-1", txId: `tx-${i}`, amount: 100 * i, status: "pending" }
        );
      }

      // MATCH (t:CC_Transaction)-[:PART_OF]->(bs:CC_BankStatement {id: $statementId})
      // RETURN COUNT(t) as txCount
      const result = exec(
        `MATCH (t:CC_Transaction)-[:PART_OF]->(bs:CC_BankStatement {id: $statementId})
         RETURN COUNT(t) as txCount`,
        { statementId: "stmt-1" }
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].txCount).toBe(5);
    });

    it("deletes bank statement with transactions and spending invoices", () => {
      // Setup
      exec(`CREATE (bs:CC_BankStatement {id: $id, bank: $bank})`, { id: "stmt-1", bank: "ING" });
      exec(
        `MATCH (bs:CC_BankStatement {id: $statementId})
         CREATE (t:CC_Transaction {id: $txId, amount: $amount})-[:PART_OF]->(bs)`,
        { statementId: "stmt-1", txId: "tx-1", amount: 100 }
      );
      exec(
        `MATCH (t:CC_Transaction {id: $transactionId})
         CREATE (t)-[:HAS_SPENDING_INVOICE]->(si:CC_SpendingInvoice {id: $siId, filename: $filename})`,
        { transactionId: "tx-1", siId: "si-1", filename: "invoice.pdf" }
      );

      // Step 1: Delete spending invoices
      // MATCH (t:CC_Transaction)-[:PART_OF]->(bs:CC_BankStatement {id: $statementId})
      // MATCH (t)-[:HAS_SPENDING_INVOICE]->(si:CC_SpendingInvoice)
      // DETACH DELETE si
      exec(
        `MATCH (t:CC_Transaction)-[:PART_OF]->(bs:CC_BankStatement {id: $statementId})
         MATCH (t)-[:HAS_SPENDING_INVOICE]->(si:CC_SpendingInvoice)
         DETACH DELETE si`,
        { statementId: "stmt-1" }
      );

      // Step 2: Delete transactions
      exec(
        `MATCH (t:CC_Transaction)-[:PART_OF]->(bs:CC_BankStatement {id: $statementId})
         DETACH DELETE t`,
        { statementId: "stmt-1" }
      );

      // Step 3: Delete statement
      exec(`MATCH (bs:CC_BankStatement {id: $statementId}) DETACH DELETE bs`, {
        statementId: "stmt-1",
      });

      // Verify all deleted
      expect(
        exec(`MATCH (bs:CC_BankStatement {id: $id}) RETURN bs`, { id: "stmt-1" }).data
      ).toHaveLength(0);
      expect(
        exec(`MATCH (t:CC_Transaction {id: $id}) RETURN t`, { id: "tx-1" }).data
      ).toHaveLength(0);
      expect(
        exec(`MATCH (si:CC_SpendingInvoice {id: $id}) RETURN si`, { id: "si-1" }).data
      ).toHaveLength(0);
    });
  });

  describe("Transaction Operations", () => {
    it("creates transaction linked to bank statement", () => {
      exec(`CREATE (bs:CC_BankStatement {id: $id})`, { id: "stmt-1" });

      // MATCH (bs:CC_BankStatement {id: $bankStatementId})
      // CREATE (t:CC_Transaction {...})-[:PART_OF]->(bs)
      exec(
        `MATCH (bs:CC_BankStatement {id: $bankStatementId})
         CREATE (t:CC_Transaction {
           id: $id,
           externalId: $externalId,
           date: $date,
           description: $description,
           amount: $amount,
           currency: $currency,
           type: $type,
           account: $account,
           status: $status
         })-[:PART_OF]->(bs)`,
        {
          bankStatementId: "stmt-1",
          id: "tx-1",
          externalId: "ext-123",
          date: "2024-01-15",
          description: "Payment from client",
          amount: 1500.0,
          currency: "EUR",
          type: "credit",
          account: "BE123456",
          status: "pending",
        }
      );

      const result = exec(`MATCH (t:CC_Transaction {id: $id}) RETURN t`, { id: "tx-1" });
      const tx = result.data[0].t as Record<string, unknown>;
      expect((tx.properties as Record<string, unknown>).amount).toBe(1500.0);
    });

    it("updates transaction status", () => {
      exec(`CREATE (t:CC_Transaction {id: $id, status: $status})`, {
        id: "tx-1",
        status: "pending",
      });

      // MATCH (t:CC_Transaction {id: $transactionId}) SET t.status = $status
      exec(`MATCH (t:CC_Transaction {id: $transactionId}) SET t.status = $status`, {
        transactionId: "tx-1",
        status: "matched",
      });

      const result = exec(`MATCH (t:CC_Transaction {id: $id}) RETURN t`, { id: "tx-1" });
      const tx = result.data[0].t as Record<string, unknown>;
      expect((tx.properties as Record<string, unknown>).status).toBe("matched");
    });

    it("links transaction to invoice", () => {
      exec(`CREATE (t:CC_Transaction {id: $id, amount: $amount})`, { id: "tx-1", amount: 1000 });
      exec(`CREATE (i:CC_Invoice {id: $id, invoiceNumber: $invoiceNumber})`, {
        id: "inv-1",
        invoiceNumber: "INV-001",
      });

      // Remove any existing match
      exec(
        `MATCH (t:CC_Transaction {id: $transactionId})-[r:MATCHED_WITH]->()
         DELETE r`,
        { transactionId: "tx-1" }
      );

      // Create new link
      exec(
        `MATCH (t:CC_Transaction {id: $transactionId})
         MATCH (i:CC_Invoice {id: $invoiceId})
         CREATE (t)-[:MATCHED_WITH]->(i)`,
        { transactionId: "tx-1", invoiceId: "inv-1" }
      );

      // MATCH (t:CC_Transaction {id: $transactionId})-[:MATCHED_WITH]->(i:CC_Invoice)
      // RETURN i.id as matchedInvoiceId, i.invoiceNumber as matchedInvoiceNumber
      const result = exec(
        `MATCH (t:CC_Transaction {id: $transactionId})-[:MATCHED_WITH]->(i:CC_Invoice)
         RETURN i.id as matchedInvoiceId, i.invoiceNumber as matchedInvoiceNumber`,
        { transactionId: "tx-1" }
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].matchedInvoiceId).toBe("inv-1");
      expect(result.data[0].matchedInvoiceNumber).toBe("INV-001");
    });

    it("creates spending invoice for transaction", () => {
      exec(`CREATE (t:CC_Transaction {id: $id})`, { id: "tx-1" });

      // Remove existing spending invoice if any
      exec(
        `MATCH (t:CC_Transaction {id: $transactionId})-[:HAS_SPENDING_INVOICE]->(si:CC_SpendingInvoice)
         DETACH DELETE si`,
        { transactionId: "tx-1" }
      );

      // Create spending invoice
      exec(
        `MATCH (t:CC_Transaction {id: $transactionId})
         CREATE (t)-[:HAS_SPENDING_INVOICE]->(si:CC_SpendingInvoice {
           id: $id,
           filename: $filename,
           pdfPath: $pdfPath,
           uploadedAt: $uploadedAt
         })`,
        {
          transactionId: "tx-1",
          id: "si-1",
          filename: "receipt.pdf",
          pdfPath: "/uploads/receipt.pdf",
          uploadedAt: "2024-01-15",
        }
      );

      // MATCH (t:CC_Transaction {id: $transactionId})-[:HAS_SPENDING_INVOICE]->(si:CC_SpendingInvoice)
      // RETURN si.pdfPath as pdfPath
      const result = exec(
        `MATCH (t:CC_Transaction {id: $transactionId})-[:HAS_SPENDING_INVOICE]->(si:CC_SpendingInvoice)
         RETURN si.pdfPath as pdfPath`,
        { transactionId: "tx-1" }
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].pdfPath).toBe("/uploads/receipt.pdf");
    });
  });

  describe("Invoice Operations", () => {
    it("gets invoices by user with customer info", () => {
      exec(`CREATE (u:CC_User {id: $id})`, { id: "user-1" });
      exec(
        `MATCH (u:CC_User {id: $userId})
         CREATE (u)-[:HAS_CUSTOMER]->(c:CC_Customer {id: $customerId, name: $name})`,
        { userId: "user-1", customerId: "cust-1", name: "Customer Inc" }
      );
      exec(
        `MATCH (u:CC_User {id: $userId})
         MATCH (c:CC_Customer {id: $customerId})
         CREATE (u)-[:HAS_INVOICE]->(i:CC_Invoice {id: $invoiceId, invoiceNumber: $invoiceNumber})-[:BILLED_TO]->(c)`,
        { userId: "user-1", customerId: "cust-1", invoiceId: "inv-1", invoiceNumber: "INV-001" }
      );

      // MATCH (u:CC_User {id: $userId})-[:HAS_INVOICE]->(i:CC_Invoice)-[:BILLED_TO]->(c:CC_Customer)
      // RETURN i, c.id as customerId, c.name as customerName
      const result = exec(
        `MATCH (u:CC_User {id: $userId})-[:HAS_INVOICE]->(i:CC_Invoice)-[:BILLED_TO]->(c:CC_Customer)
         RETURN i, c.id as customerId, c.name as customerName`,
        { userId: "user-1" }
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].customerId).toBe("cust-1");
      expect(result.data[0].customerName).toBe("Customer Inc");
    });

    it("gets invoice items", () => {
      exec(`CREATE (i:CC_Invoice {id: $invoiceId})`, { invoiceId: "inv-1" });

      for (let i = 1; i <= 3; i++) {
        exec(
          `MATCH (i:CC_Invoice {id: $invoiceId})
           CREATE (i)-[:CONTAINS]->(item:CC_InvoiceItem {
             id: $itemId,
             label: $label,
             quantity: $quantity,
             unitPrice: $unitPrice,
             total: $total
           })`,
          {
            invoiceId: "inv-1",
            itemId: `item-${i}`,
            label: `Item ${i}`,
            quantity: i,
            unitPrice: 100,
            total: i * 100,
          }
        );
      }

      // MATCH (i:CC_Invoice {id: $invoiceId})-[:CONTAINS]->(item:CC_InvoiceItem) RETURN item
      const result = exec(
        `MATCH (i:CC_Invoice {id: $invoiceId})-[:CONTAINS]->(item:CC_InvoiceItem)
         RETURN item`,
        { invoiceId: "inv-1" }
      );

      expect(result.data).toHaveLength(3);
    });

    it("updates invoice pdf path", () => {
      exec(`CREATE (i:CC_Invoice {id: $invoiceId, invoiceNumber: $invoiceNumber})`, {
        invoiceId: "inv-1",
        invoiceNumber: "INV-001",
      });

      // MATCH (i:CC_Invoice {id: $invoiceId}) SET i.pdfPath = $pdfPath
      exec(`MATCH (i:CC_Invoice {id: $invoiceId}) SET i.pdfPath = $pdfPath`, {
        invoiceId: "inv-1",
        pdfPath: "/invoices/INV-001.pdf",
      });

      const result = exec(`MATCH (i:CC_Invoice {id: $invoiceId}) RETURN i`, {
        invoiceId: "inv-1",
      });
      const invoice = result.data[0].i as Record<string, unknown>;
      expect((invoice.properties as Record<string, unknown>).pdfPath).toBe("/invoices/INV-001.pdf");
    });

    it("updates invoice status through user relationship", () => {
      exec(`CREATE (u:CC_User {id: $id})`, { id: "user-1" });
      exec(
        `MATCH (u:CC_User {id: $userId})
         CREATE (u)-[:HAS_INVOICE]->(i:CC_Invoice {id: $invoiceId, status: $status})`,
        { userId: "user-1", invoiceId: "inv-1", status: "draft" }
      );

      // MATCH (u:CC_User {id: $userId})-[:HAS_INVOICE]->(i:CC_Invoice {id: $invoiceId})
      // SET i.status = $status
      exec(
        `MATCH (u:CC_User {id: $userId})-[:HAS_INVOICE]->(i:CC_Invoice {id: $invoiceId})
         SET i.status = $status`,
        { userId: "user-1", invoiceId: "inv-1", status: "sent" }
      );

      const result = exec(
        `MATCH (u:CC_User {id: $userId})-[:HAS_INVOICE]->(i:CC_Invoice {id: $invoiceId})
         RETURN i`,
        { userId: "user-1", invoiceId: "inv-1" }
      );
      const invoice = result.data[0].i as Record<string, unknown>;
      expect((invoice.properties as Record<string, unknown>).status).toBe("sent");
    });

    it("deletes invoice with items", () => {
      exec(`CREATE (u:CC_User {id: $id})`, { id: "user-1" });
      exec(
        `MATCH (u:CC_User {id: $userId})
         CREATE (u)-[:HAS_INVOICE]->(i:CC_Invoice {id: $invoiceId})`,
        { userId: "user-1", invoiceId: "inv-1" }
      );
      exec(
        `MATCH (i:CC_Invoice {id: $invoiceId})
         CREATE (i)-[:CONTAINS]->(item:CC_InvoiceItem {id: $itemId})`,
        { invoiceId: "inv-1", itemId: "item-1" }
      );

      // Delete items first
      exec(
        `MATCH (i:CC_Invoice {id: $invoiceId})-[:CONTAINS]->(item:CC_InvoiceItem)
         DETACH DELETE item`,
        { invoiceId: "inv-1" }
      );

      // Delete invoice
      exec(
        `MATCH (u:CC_User {id: $userId})-[:HAS_INVOICE]->(i:CC_Invoice {id: $invoiceId})
         DETACH DELETE i`,
        { userId: "user-1", invoiceId: "inv-1" }
      );

      expect(
        exec(`MATCH (i:CC_Invoice {id: $id}) RETURN i`, { id: "inv-1" }).data
      ).toHaveLength(0);
      expect(
        exec(`MATCH (item:CC_InvoiceItem {id: $id}) RETURN item`, { id: "item-1" }).data
      ).toHaveLength(0);
    });
  });

  describe("Invoice Sequence Operations", () => {
    it("gets sequences by user id", () => {
      exec(`CREATE (u:CC_User {id: $id})`, { id: "user-1" });

      for (let i = 1; i <= 2; i++) {
        exec(
          `MATCH (u:CC_User {id: $userId})
           CREATE (u)-[:HAS_SEQUENCE]->(s:CC_InvoiceSequence {
             id: $sequenceId,
             prefix: $prefix,
             lastNumber: $lastNumber
           })`,
          {
            userId: "user-1",
            sequenceId: `seq-${i}`,
            prefix: `PREFIX-${i}`,
            lastNumber: 0,
          }
        );
      }

      // MATCH (u:CC_User {id: $userId})-[:HAS_SEQUENCE]->(s:CC_InvoiceSequence) RETURN s
      const result = exec(
        `MATCH (u:CC_User {id: $userId})-[:HAS_SEQUENCE]->(s:CC_InvoiceSequence)
         RETURN s`,
        { userId: "user-1" }
      );

      expect(result.data).toHaveLength(2);
    });

    it("gets sequence by id", () => {
      exec(`CREATE (u:CC_User {id: $id})`, { id: "user-1" });
      exec(
        `MATCH (u:CC_User {id: $userId})
         CREATE (u)-[:HAS_SEQUENCE]->(s:CC_InvoiceSequence {id: $sequenceId, prefix: $prefix, lastNumber: $lastNumber})`,
        { userId: "user-1", sequenceId: "seq-1", prefix: "INV", lastNumber: 42 }
      );

      // MATCH (u:CC_User {id: $userId})-[:HAS_SEQUENCE]->(s:CC_InvoiceSequence {id: $sequenceId}) RETURN s
      const result = exec(
        `MATCH (u:CC_User {id: $userId})-[:HAS_SEQUENCE]->(s:CC_InvoiceSequence {id: $sequenceId})
         RETURN s`,
        { userId: "user-1", sequenceId: "seq-1" }
      );

      expect(result.data).toHaveLength(1);
      const seq = result.data[0].s as Record<string, unknown>;
      expect((seq.properties as Record<string, unknown>).prefix).toBe("INV");
    });

    it("updates sequence", () => {
      exec(`CREATE (u:CC_User {id: $id})`, { id: "user-1" });
      exec(
        `MATCH (u:CC_User {id: $userId})
         CREATE (u)-[:HAS_SEQUENCE]->(s:CC_InvoiceSequence {id: $sequenceId, prefix: $prefix, lastNumber: $lastNumber})`,
        { userId: "user-1", sequenceId: "seq-1", prefix: "OLD", lastNumber: 0 }
      );

      // MATCH (u:CC_User {id: $userId})-[:HAS_SEQUENCE]->(s:CC_InvoiceSequence {id: $sequenceId})
      // SET s.prefix = $prefix, s.lastNumber = $lastNumber
      exec(
        `MATCH (u:CC_User {id: $userId})-[:HAS_SEQUENCE]->(s:CC_InvoiceSequence {id: $sequenceId})
         SET s.prefix = $prefix, s.lastNumber = $lastNumber`,
        { userId: "user-1", sequenceId: "seq-1", prefix: "NEW", lastNumber: 10 }
      );

      const result = exec(
        `MATCH (s:CC_InvoiceSequence {id: $sequenceId}) RETURN s`,
        { sequenceId: "seq-1" }
      );
      const seq = result.data[0].s as Record<string, unknown>;
      expect((seq.properties as Record<string, unknown>).prefix).toBe("NEW");
      expect((seq.properties as Record<string, unknown>).lastNumber).toBe(10);
    });

    it("increments sequence", () => {
      exec(`CREATE (s:CC_InvoiceSequence {id: $sequenceId, prefix: $prefix, lastNumber: $lastNumber})`, {
        sequenceId: "seq-1",
        prefix: "INV",
        lastNumber: 5,
      });

      // Get current value
      // MATCH (s:CC_InvoiceSequence {id: $sequenceId}) RETURN s.lastNumber as lastNumber
      const current = exec(
        `MATCH (s:CC_InvoiceSequence {id: $sequenceId})
         RETURN s.lastNumber as lastNumber`,
        { sequenceId: "seq-1" }
      );
      expect(current.data[0].lastNumber).toBe(5);

      // Increment
      // MATCH (s:CC_InvoiceSequence {id: $sequenceId}) SET s.lastNumber = $newNumber
      exec(
        `MATCH (s:CC_InvoiceSequence {id: $sequenceId})
         SET s.lastNumber = $newNumber`,
        { sequenceId: "seq-1", newNumber: 6 }
      );

      const updated = exec(
        `MATCH (s:CC_InvoiceSequence {id: $sequenceId})
         RETURN s.lastNumber as lastNumber`,
        { sequenceId: "seq-1" }
      );
      expect(updated.data[0].lastNumber).toBe(6);
    });

    it("deletes sequence", () => {
      exec(`CREATE (u:CC_User {id: $id})`, { id: "user-1" });
      exec(
        `MATCH (u:CC_User {id: $userId})
         CREATE (u)-[:HAS_SEQUENCE]->(s:CC_InvoiceSequence {id: $sequenceId, prefix: $prefix})`,
        { userId: "user-1", sequenceId: "seq-1", prefix: "INV" }
      );

      // MATCH (u:CC_User {id: $userId})-[:HAS_SEQUENCE]->(s:CC_InvoiceSequence {id: $sequenceId})
      // DETACH DELETE s
      exec(
        `MATCH (u:CC_User {id: $userId})-[:HAS_SEQUENCE]->(s:CC_InvoiceSequence {id: $sequenceId})
         DETACH DELETE s`,
        { userId: "user-1", sequenceId: "seq-1" }
      );

      const result = exec(`MATCH (s:CC_InvoiceSequence {id: $id}) RETURN s`, {
        id: "seq-1",
      });
      expect(result.data).toHaveLength(0);
    });
  });

  describe("Multi-hop Queries", () => {
    it("handles MATCH...MATCH with shared variable (transactions by report)", () => {
      // Setup: Report -> BankStatement <- Transaction
      exec(`CREATE (r:CC_MonthlyReport {id: $id})`, { id: "report-1" });
      exec(
        `MATCH (r:CC_MonthlyReport {id: $reportId})
         CREATE (r)-[:HAS_BANK_STATEMENT]->(bs:CC_BankStatement {id: $bsId})`,
        { reportId: "report-1", bsId: "bs-1" }
      );
      exec(
        `MATCH (bs:CC_BankStatement {id: $bsId})
         CREATE (t:CC_Transaction {id: $txId, amount: $amount})-[:PART_OF]->(bs)`,
        { bsId: "bs-1", txId: "tx-1", amount: 500 }
      );

      // MATCH (r:CC_MonthlyReport {id: $reportId})-[:HAS_BANK_STATEMENT]->(bs:CC_BankStatement)
      // MATCH (t:CC_Transaction)-[:PART_OF]->(bs)
      // RETURN t, bs.id as bankStatementId
      const result = exec(
        `MATCH (r:CC_MonthlyReport {id: $reportId})-[:HAS_BANK_STATEMENT]->(bs:CC_BankStatement)
         MATCH (t:CC_Transaction)-[:PART_OF]->(bs)
         RETURN t, bs.id as bankStatementId`,
        { reportId: "report-1" }
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].bankStatementId).toBe("bs-1");
      const tx = result.data[0].t as Record<string, unknown>;
      expect((tx.properties as Record<string, unknown>).amount).toBe(500);
    });

    it("handles three-hop pattern (User -> Invoice -> Customer)", () => {
      exec(`CREATE (u:CC_User {id: $id})`, { id: "user-1" });
      exec(
        `MATCH (u:CC_User {id: $userId})
         CREATE (u)-[:HAS_CUSTOMER]->(c:CC_Customer {id: $customerId, name: $name})`,
        { userId: "user-1", customerId: "cust-1", name: "Customer Inc" }
      );
      exec(
        `MATCH (u:CC_User {id: $userId})
         MATCH (c:CC_Customer {id: $customerId})
         CREATE (u)-[:HAS_INVOICE]->(i:CC_Invoice {id: $invoiceId, invoiceNumber: $invoiceNumber})-[:BILLED_TO]->(c)`,
        { userId: "user-1", customerId: "cust-1", invoiceId: "inv-1", invoiceNumber: "INV-001" }
      );

      // Three-hop query to get invoices for specific customer
      const result = exec(
        `MATCH (u:CC_User {id: $userId})-[:HAS_INVOICE]->(i:CC_Invoice)-[:BILLED_TO]->(c:CC_Customer {id: $customerId})
         RETURN i, c.id as customerId, c.name as customerName`,
        { userId: "user-1", customerId: "cust-1" }
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].customerId).toBe("cust-1");
      expect(result.data[0].customerName).toBe("Customer Inc");
    });
  });

  describe("Connection Verification", () => {
    it("handles RETURN 1 for connection verification", () => {
      // RETURN 1
      const result = exec("RETURN 1");

      expect(result.data).toHaveLength(1);
      expect(result.data[0].expr).toBe(1);
    });
  });
});
