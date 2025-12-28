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

  /**
   * Additional tests based on patterns from /tmp/cypher_queries.json
   * These patterns are from real production usage in sellersuite application
   */
  describe("Sellersuite Query Patterns", () => {
    describe("Node comparison in WHERE clause", () => {
      it("finds duplicate nodes by comparing i <> i2", () => {
        // Pattern: MATCH (i:Image), (i2:Image) WHERE i <> i2 AND i.image_id = i2.image_id
        // This pattern finds nodes with duplicate property values
        exec("CREATE (i1:Image {image_id: 'img-001', name: 'First'})");
        exec("CREATE (i2:Image {image_id: 'img-001', name: 'Second'})");
        exec("CREATE (i3:Image {image_id: 'img-002', name: 'Third'})");

        // Find images that share the same image_id but are different nodes
        const result = exec(`
          MATCH (i:Image), (i2:Image)
          WHERE i <> i2 AND i.image_id = i2.image_id
          RETURN DISTINCT i.image_id as image_id
        `);

        expect(result.data).toHaveLength(1);
        expect(result.data[0].image_id).toBe("img-001");
      });
    });

    describe("COLLECT with object construction", () => {
      it("collects properties into objects", () => {
        // Pattern: collect({ intellinaut_id: u.user_id, first_name: u.first_name, ... })
        exec("CREATE (u:User {user_id: 'u1', first_name: 'Alice', last_name: 'Smith', email: 'alice@example.com'})");
        exec("CREATE (u:User {user_id: 'u2', first_name: 'Bob', last_name: 'Jones', email: 'bob@example.com'})");
        exec("CREATE (c:Company {company_id: 'c1', name: 'Acme'})");
        
        // Link users to company
        const users = exec("MATCH (u:User) RETURN u.user_id, id(u) as uid").data;
        const company = exec("MATCH (c:Company) RETURN id(c) as cid").data[0];
        
        for (const user of users) {
          db.insertEdge(`admin-${user.u_user_id}`, "IS_ADMIN", user.uid as string, company.cid as string);
        }

        // Collect users into objects
        const result = exec(`
          MATCH (u:User)-[:IS_ADMIN]->(c:Company)
          RETURN c.company_id as company_id,
                 collect({
                   intellinaut_id: u.user_id,
                   first_name: u.first_name,
                   last_name: u.last_name,
                   email: u.email
                 }) as intellinauts
        `);

        expect(result.data).toHaveLength(1);
        expect(result.data[0].company_id).toBe("c1");
        const intellinauts = result.data[0].intellinauts as Array<Record<string, unknown>>;
        expect(intellinauts).toHaveLength(2);
        expect(intellinauts.some(i => i.first_name === "Alice")).toBe(true);
        expect(intellinauts.some(i => i.first_name === "Bob")).toBe(true);
      });
    });

    describe("ID() function with alias in RETURN", () => {
      it("returns node ID with custom alias", () => {
        // Pattern: MATCH (n) RETURN n, ID(n) as nid
        exec("CREATE (n:Node {name: 'Test'})");

        const result = exec("MATCH (n:Node) RETURN n, ID(n) as nid");

        expect(result.data).toHaveLength(1);
        expect(result.data[0].nid).toBeDefined();
        expect(typeof result.data[0].nid).toBe("string");
        // UUID format check
        expect(result.data[0].nid).toMatch(
          /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i
        );
      });

      it("returns edge ID with custom alias", () => {
        // Pattern: MATCH ()-[r]->() RETURN r, ID(r) as rid
        exec("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})");

        const result = exec("MATCH ()-[r:KNOWS]->() RETURN ID(r) as rid");

        expect(result.data).toHaveLength(1);
        expect(result.data[0].rid).toBeDefined();
      });
    });

    describe("Relationship property filters in pattern", () => {
      it("filters by relationship property in MATCH pattern", () => {
        // Pattern: (p:Product)-[:PRODUCT_INFO{market_place:$market_place}]->(pi:ProductInfo)
        exec("CREATE (p:Product {product_id: 'p1', sku: 'SKU001'})");
        
        // Create product infos with different market_places
        const product = exec("MATCH (p:Product {product_id: 'p1'}) RETURN id(p) as pid").data[0];
        
        exec("CREATE (pi:ProductInfo {title: 'US Product', price: 99.99})");
        exec("CREATE (pi:ProductInfo {title: 'EU Product', price: 89.99})");
        
        const productInfos = exec("MATCH (pi:ProductInfo) RETURN pi.title, id(pi) as piid").data;
        
        const usInfo = productInfos.find(pi => pi.pi_title === "US Product");
        const euInfo = productInfos.find(pi => pi.pi_title === "EU Product");
        
        db.insertEdge("pi-us", "PRODUCT_INFO", product.pid as string, usInfo?.piid as string, { market_place: "us" });
        db.insertEdge("pi-eu", "PRODUCT_INFO", product.pid as string, euInfo?.piid as string, { market_place: "eu" });

        // Query with relationship property filter
        const result = exec(`
          MATCH (p:Product {product_id: $product_id})-[r:PRODUCT_INFO {market_place: $market_place}]->(pi:ProductInfo)
          RETURN pi.title as title, pi.price as price
        `, { product_id: "p1", market_place: "us" });

        expect(result.data).toHaveLength(1);
        expect(result.data[0].title).toBe("US Product");
        expect(result.data[0].price).toBe(99.99);
      });
    });

    describe("DELETE relationship variable", () => {
      it("deletes relationship by variable without DETACH", () => {
        // Pattern: MATCH (pi:ProductInfo)-[s:SOLD_BY]->(a) DELETE s
        exec("CREATE (pi:ProductInfo {title: 'Product'})");
        exec("CREATE (a:AmazonAccount {merchant_id: 'M001'})");
        
        const pi = exec("MATCH (pi:ProductInfo) RETURN id(pi) as piid").data[0];
        const acc = exec("MATCH (a:AmazonAccount) RETURN id(a) as aid").data[0];
        
        db.insertEdge("sold-by-1", "SOLD_BY", pi.piid as string, acc.aid as string);

        // Verify relationship exists
        let check = exec("MATCH (pi:ProductInfo)-[:SOLD_BY]->(a:AmazonAccount) RETURN pi.title");
        expect(check.data).toHaveLength(1);

        // Delete relationship only
        exec("MATCH (pi:ProductInfo)-[s:SOLD_BY]->(a) DELETE s");

        // Verify relationship is gone but nodes remain
        check = exec("MATCH (pi:ProductInfo)-[:SOLD_BY]->(a:AmazonAccount) RETURN pi.title");
        expect(check.data).toHaveLength(0);
        
        // Nodes should still exist
        expect(exec("MATCH (pi:ProductInfo) RETURN pi").data).toHaveLength(1);
        expect(exec("MATCH (a:AmazonAccount) RETURN a").data).toHaveLength(1);
      });
    });

    describe("Match all nodes without label", () => {
      it("matches all nodes with (o) pattern", () => {
        // Pattern: MATCH (o) RETURN count(o) as count
        exec("CREATE (p:Person {name: 'Alice'})");
        exec("CREATE (c:Company {name: 'Acme'})");
        exec("CREATE (i:Invoice {id: 'inv-1'})");

        const result = exec("MATCH (o) RETURN count(o) as count");

        expect(result.data).toHaveLength(1);
        expect(result.data[0].count).toBe(3);
      });
    });

    describe("Match all relationships without type", () => {
      it("matches all relationships with (m)-[r]->(o) pattern", () => {
        // Pattern: MATCH (m)-[r]->(o) RETURN count(r) as count
        exec("CREATE (a:Person {name: 'Alice'})");
        exec("CREATE (b:Person {name: 'Bob'})");
        exec("CREATE (c:Company {name: 'Acme'})");
        
        const alice = exec("MATCH (p:Person {name: 'Alice'}) RETURN id(p) as pid").data[0];
        const bob = exec("MATCH (p:Person {name: 'Bob'}) RETURN id(p) as pid").data[0];
        const acme = exec("MATCH (c:Company) RETURN id(c) as cid").data[0];
        
        db.insertEdge("r1", "KNOWS", alice.pid as string, bob.pid as string);
        db.insertEdge("r2", "WORKS_AT", alice.pid as string, acme.cid as string);
        db.insertEdge("r3", "WORKS_AT", bob.pid as string, acme.cid as string);

        const result = exec("MATCH (m)-[r]->(o) RETURN count(r) as count");

        expect(result.data).toHaveLength(1);
        expect(result.data[0].count).toBe(3);
      });
    });

    describe("DETACH DELETE with matched relationship", () => {
      it("detach deletes matched nodes through relationship pattern", () => {
        // Pattern: MATCH (u:User{email:$email})-[prev_admin:IS_ADMIN]->(c:Company) DETACH DELETE prev_admin, c
        exec("CREATE (u:User {email: 'test@example.com', name: 'Test User'})");
        exec("CREATE (c:Company {company_id: 'c1', name: 'Old Company'})");
        
        const user = exec("MATCH (u:User) RETURN id(u) as uid").data[0];
        const company = exec("MATCH (c:Company) RETURN id(c) as cid").data[0];
        
        db.insertEdge("admin-rel", "IS_ADMIN", user.uid as string, company.cid as string);

        // Verify setup
        let check = exec("MATCH (u:User)-[:IS_ADMIN]->(c:Company) RETURN u.email, c.name");
        expect(check.data).toHaveLength(1);

        // Detach delete the relationship and company
        exec(`
          MATCH (u:User {email: $email})-[prev_admin:IS_ADMIN]->(c:Company)
          DETACH DELETE prev_admin, c
        `, { email: "test@example.com" });

        // User should remain
        expect(exec("MATCH (u:User) RETURN u").data).toHaveLength(1);
        // Company should be gone
        expect(exec("MATCH (c:Company) RETURN c").data).toHaveLength(0);
        // Relationship should be gone
        check = exec("MATCH (u:User)-[:IS_ADMIN]->(c:Company) RETURN u");
        expect(check.data).toHaveLength(0);
      });
    });

    describe("Variable-length paths with edge type", () => {
      it("matches variable-length path with specific edge type", () => {
        // Pattern: (c)-[*1..3]->(p:Product)
        exec("CREATE (c:Company {company_id: 'c1'})");
        exec("CREATE (cat:Category {name: 'Electronics'})");
        exec("CREATE (p:Product {name: 'Laptop'})");
        
        const company = exec("MATCH (c:Company) RETURN id(c) as cid").data[0];
        const category = exec("MATCH (cat:Category) RETURN id(cat) as catid").data[0];
        const product = exec("MATCH (p:Product) RETURN id(p) as pid").data[0];
        
        db.insertEdge("r1", "HAS_CATEGORY", company.cid as string, category.catid as string);
        db.insertEdge("r2", "CONTAINS", category.catid as string, product.pid as string);

        // Find products reachable within 1-3 hops from company
        const result = exec(`
          MATCH (c:Company {company_id: $company_id})-[*1..3]->(p:Product)
          RETURN count(p) as total
        `, { company_id: "c1" });

        expect(result.data).toHaveLength(1);
        expect(result.data[0].total).toBe(1);
      });
    });

    describe("ORDER BY with count aggregation", () => {
      it("orders by count aggregation", () => {
        // Pattern: RETURN count(u) as total, user.email as email ORDER BY total
        exec("CREATE (c:Company {company_id: 'c1'})");
        exec("CREATE (c:Company {company_id: 'c2'})");
        exec("CREATE (f:Feature {feature: 'export'})");
        
        const companies = exec("MATCH (c:Company) RETURN c.company_id, id(c) as cid").data;
        const feature = exec("MATCH (f:Feature) RETURN id(f) as fid").data[0];
        
        // Company 1 uses feature 3 times
        for (let i = 0; i < 3; i++) {
          db.insertEdge(`used-c1-${i}`, "USED", companies[0].cid as string, feature.fid as string);
        }
        // Company 2 uses feature 1 time
        db.insertEdge("used-c2-0", "USED", companies[1].cid as string, feature.fid as string);

        // Create users for each company
        exec("CREATE (u:User {email: 'user1@c1.com', company_id: 'c1'})");
        exec("CREATE (u:User {email: 'user2@c2.com', company_id: 'c2'})");
        
        const users = exec("MATCH (u:User) RETURN u.email, u.company_id, id(u) as uid").data;
        for (const user of users) {
          const company = companies.find(c => c.c_company_id === user.u_company_id);
          if (company) {
            db.insertEdge(`admin-${user.u_email}`, "IS_ADMIN", user.uid as string, company.cid as string);
          }
        }

        // Query feature usage with ORDER BY count
        const result = exec(`
          MATCH (c:Company)-[u:USED]->(f:Feature {feature: $feature}),
                (user:User)-[:IS_ADMIN]->(c)
          RETURN count(u) as total, user.email as email
          ORDER BY total
        `, { feature: "export" });

        expect(result.data.length).toBeGreaterThan(0);
        // Results should be ordered by total ascending
        for (let i = 1; i < result.data.length; i++) {
          expect(result.data[i].total as number).toBeGreaterThanOrEqual(result.data[i - 1].total as number);
        }
      });
    });

    describe("NOT NULL check with IS NOT NULL", () => {
      it("filters by IS NOT NULL in WHERE clause", () => {
        // Pattern: WHERE NOT o.comment_request_date IS NULL
        exec("CREATE (o:Order {order_id: 'o1', comment_request_date: '2024-01-15'})");
        exec("CREATE (o:Order {order_id: 'o2'})"); // No comment_request_date
        exec("CREATE (o:Order {order_id: 'o3', comment_request_date: '2024-01-20'})");

        const result = exec(`
          MATCH (o:Order)
          WHERE NOT o.comment_request_date IS NULL
          RETURN o.order_id as order_id
        `);

        expect(result.data).toHaveLength(2);
        const orderIds = result.data.map(r => r.order_id);
        expect(orderIds).toContain("o1");
        expect(orderIds).toContain("o3");
        expect(orderIds).not.toContain("o2");
      });
    });

    describe("Multiple relationship patterns in CREATE", () => {
      it("creates multiple relationships in single CREATE", () => {
        // Pattern: CREATE (k:Keyword)-[r1:RELATED_KEYWORDS]->(rk1:RelatedKeyword), (k)-[r2:RELATED_KEYWORDS]->(rk2:RelatedKeyword)
        const result = exec(`
          CREATE (k:Keyword {lang: $lang, keyword: $keyword})-[r1:RELATED_KEYWORDS]->(rk1:RelatedKeyword {keyword: $duplicate_keyword}),
                 (k)-[r2:RELATED_KEYWORDS]->(rk2:RelatedKeyword {keyword: $duplicate_keyword})
          RETURN k
        `, { lang: "en", keyword: "computer", duplicate_keyword: "laptop" });

        expect(result.success).toBe(true);

        // Verify keyword was created
        const keywords = exec("MATCH (k:Keyword) RETURN k");
        expect(keywords.data).toHaveLength(1);

        // Verify related keywords were created
        const relatedKeywords = exec("MATCH (rk:RelatedKeyword) RETURN rk");
        expect(relatedKeywords.data).toHaveLength(2);

        // Verify relationships exist
        const rels = exec("MATCH (k:Keyword)-[:RELATED_KEYWORDS]->(rk:RelatedKeyword) RETURN rk");
        expect(rels.data).toHaveLength(2);
      });
    });

    describe("WITH clause with UNWIND roundtrip", () => {
      it("handles COLLECT followed by UNWIND", () => {
        // Pattern: WITH COLLECT(n.name) AS names UNWIND names AS name RETURN name
        exec("CREATE (p:Person {name: 'Alice'})");
        exec("CREATE (p:Person {name: 'Bob'})");
        exec("CREATE (p:Person {name: 'Charlie'})");

        const result = exec(`
          MATCH (n:Person)
          WITH COLLECT(n.name) AS names
          UNWIND names AS name
          RETURN name
        `);

        expect(result.data).toHaveLength(3);
        const names = result.data.map(r => r.name);
        expect(names).toContain("Alice");
        expect(names).toContain("Bob");
        expect(names).toContain("Charlie");
      });
    });

    describe("Multi-hop with ORDER BY and LIMIT", () => {
      it("handles multi-hop traversal returning node and relationship with ORDER BY", () => {
        // Pattern: MATCH (u:BF_User {id: $userId})-[:BF_LEARNS]->(l:BF_Language)-[rel:BF_HAS_FLASHCARD]->(f:BF_Flashcard)
        //          RETURN f, rel ORDER BY f.created_at DESC
        exec("CREATE (u:BF_User {id: 'user-1'})");
        exec(`
          MATCH (u:BF_User {id: $userId})
          CREATE (u)-[:BF_LEARNS]->(l:BF_Language {language: 'Spanish'})
        `, { userId: "user-1" });

        // Create flashcards with different timestamps
        const timestamps = [
          "2024-01-15T10:00:00Z",
          "2024-01-16T10:00:00Z",
          "2024-01-14T10:00:00Z",
        ];

        for (let i = 0; i < timestamps.length; i++) {
          exec(`
            MATCH (u:BF_User {id: $userId})-[:BF_LEARNS]->(l:BF_Language)
            CREATE (l)-[:BF_HAS_FLASHCARD {difficulty: $difficulty}]->(f:BF_Flashcard {
              id: $flashcardId,
              front: $front,
              back: $back,
              created_at: $createdAt
            })
          `, {
            userId: "user-1",
            flashcardId: `flashcard-${i + 1}`,
            front: `Front ${i + 1}`,
            back: `Back ${i + 1}`,
            createdAt: timestamps[i],
            difficulty: i + 1,
          });
        }

        // Query: return both node and relationship, ordered by node property
        const result = exec(`
          MATCH (u:BF_User {id: $userId})-[:BF_LEARNS]->(l:BF_Language)-[rel:BF_HAS_FLASHCARD]->(f:BF_Flashcard)
          RETURN f, rel
          ORDER BY f.created_at DESC
        `, { userId: "user-1" });

        expect(result.data).toHaveLength(3);
        
        // Verify order is descending by created_at
        const flashcards = result.data.map(r => {
          const f = r.f as Record<string, unknown>;
          const props = f.properties as Record<string, unknown>;
          return { id: props.id, created_at: props.created_at };
        });
        
        // Should be ordered: 2024-01-16, 2024-01-15, 2024-01-14
        expect(flashcards[0].id).toBe("flashcard-2"); // 2024-01-16
        expect(flashcards[1].id).toBe("flashcard-1"); // 2024-01-15
        expect(flashcards[2].id).toBe("flashcard-3"); // 2024-01-14

        // Verify rel is also returned
        expect(result.data[0].rel).toBeDefined();
      });

      it("handles multi-hop traversal with ORDER BY DESC and LIMIT", () => {
        // Pattern: MATCH (u:BF_User {id: $userId})-[:BF_LEARNS]->(l:BF_Language)-[:BF_HAS_CHAT]->(c:BF_Chat)
        //          RETURN c ORDER BY c.updated_at DESC LIMIT 20
        exec("CREATE (u:BF_User {id: 'user-1'})");
        exec(`
          MATCH (u:BF_User {id: $userId})
          CREATE (u)-[:BF_LEARNS]->(l:BF_Language {language: 'Spanish', proficiency: 'beginner'})
        `, { userId: "user-1" });

        // Create multiple chats with different timestamps
        const timestamps = [
          "2024-01-15T10:00:00Z",
          "2024-01-16T10:00:00Z",
          "2024-01-14T10:00:00Z",
          "2024-01-17T10:00:00Z",
          "2024-01-13T10:00:00Z",
        ];

        for (let i = 0; i < timestamps.length; i++) {
          exec(`
            MATCH (u:BF_User {id: $userId})-[:BF_LEARNS]->(l:BF_Language)
            CREATE (l)-[:BF_HAS_CHAT]->(c:BF_Chat {
              id: $chatId,
              title: $title,
              updated_at: $updatedAt
            })
          `, {
            userId: "user-1",
            chatId: `chat-${i + 1}`,
            title: `Chat ${i + 1}`,
            updatedAt: timestamps[i],
          });
        }

        // Query: get chats ordered by updated_at DESC with LIMIT
        const result = exec(`
          MATCH (u:BF_User {id: $userId})-[:BF_LEARNS]->(l:BF_Language)-[:BF_HAS_CHAT]->(c:BF_Chat)
          RETURN c
          ORDER BY c.updated_at DESC
          LIMIT 20
        `, { userId: "user-1" });

        expect(result.data).toHaveLength(5);
        
        // Verify order is descending by updated_at
        const chats = result.data.map(r => {
          const chat = r.c as Record<string, unknown>;
          const props = chat.properties as Record<string, unknown>;
          return { id: props.id, updated_at: props.updated_at };
        });
        
        // Should be ordered: 2024-01-17, 2024-01-16, 2024-01-15, 2024-01-14, 2024-01-13
        expect(chats[0].id).toBe("chat-4"); // 2024-01-17
        expect(chats[1].id).toBe("chat-2"); // 2024-01-16
        expect(chats[2].id).toBe("chat-1"); // 2024-01-15
        expect(chats[3].id).toBe("chat-3"); // 2024-01-14
        expect(chats[4].id).toBe("chat-5"); // 2024-01-13
      });
    });

    describe("DISTINCT in aggregations", () => {
      it("handles count(DISTINCT n.property)", () => {
        // Pattern: RETURN count(DISTINCT n.name)
        exec("CREATE (p:Person {name: 'Alice', city: 'NYC'})");
        exec("CREATE (p:Person {name: 'Bob', city: 'NYC'})");
        exec("CREATE (p:Person {name: 'Alice', city: 'LA'})"); // Duplicate name

        const result = exec(`
          MATCH (p:Person)
          RETURN count(DISTINCT p.name) as uniqueNames
        `);

        expect(result.data).toHaveLength(1);
        expect(result.data[0].uniqueNames).toBe(2); // Alice and Bob
      });

      it("handles collect(DISTINCT n.property)", () => {
        // Pattern: RETURN collect(DISTINCT n.category)
        exec("CREATE (p:Product {name: 'Laptop', category: 'Electronics'})");
        exec("CREATE (p:Product {name: 'Phone', category: 'Electronics'})");
        exec("CREATE (p:Product {name: 'Shirt', category: 'Clothing'})");
        exec("CREATE (p:Product {name: 'Pants', category: 'Clothing'})");

        const result = exec(`
          MATCH (p:Product)
          RETURN collect(DISTINCT p.category) as categories
        `);

        expect(result.data).toHaveLength(1);
        const categories = result.data[0].categories as string[];
        expect(categories).toHaveLength(2);
        expect(categories).toContain("Electronics");
        expect(categories).toContain("Clothing");
      });

      it("handles sum(DISTINCT n.property)", () => {
        // Pattern: RETURN sum(DISTINCT n.value)
        exec("CREATE (o:Order {id: '1', amount: 100})");
        exec("CREATE (o:Order {id: '2', amount: 200})");
        exec("CREATE (o:Order {id: '3', amount: 100})"); // Duplicate amount

        const result = exec(`
          MATCH (o:Order)
          RETURN sum(DISTINCT o.amount) as totalUniqueAmounts
        `);

        expect(result.data).toHaveLength(1);
        expect(result.data[0].totalUniqueAmounts).toBe(300); // 100 + 200 (not 400)
      });

      it("handles count(DISTINCT) with relationship grouping", () => {
        // Pattern: RETURN count(DISTINCT property) with implicit grouping via relationships
        // Note: Implicit GROUP BY from non-aggregated columns is not yet implemented.
        // This test verifies DISTINCT works in a simpler case using relationship patterns.
        exec("CREATE (d:Department {name: 'Engineering'})");
        exec("CREATE (d:Department {name: 'Marketing'})");
        
        // Engineering department employees
        exec(`MATCH (d:Department {name: 'Engineering'}) 
              CREATE (d)-[:HAS_EMPLOYEE]->(e:Employee {name: 'Alice', skill: 'Python'})`);
        exec(`MATCH (d:Department {name: 'Engineering'}) 
              CREATE (d)-[:HAS_EMPLOYEE]->(e:Employee {name: 'Bob', skill: 'Python'})`);
        exec(`MATCH (d:Department {name: 'Engineering'}) 
              CREATE (d)-[:HAS_EMPLOYEE]->(e:Employee {name: 'Charlie', skill: 'Java'})`);
        
        // Marketing department employees
        exec(`MATCH (d:Department {name: 'Marketing'}) 
              CREATE (d)-[:HAS_EMPLOYEE]->(e:Employee {name: 'Diana', skill: 'SEO'})`);
        exec(`MATCH (d:Department {name: 'Marketing'}) 
              CREATE (d)-[:HAS_EMPLOYEE]->(e:Employee {name: 'Eve', skill: 'SEO'})`);

        // Query for Engineering department's unique skills
        const result = exec(`
          MATCH (d:Department {name: 'Engineering'})-[:HAS_EMPLOYEE]->(e:Employee)
          RETURN count(DISTINCT e.skill) as uniqueSkills
        `);

        expect(result.data).toHaveLength(1);
        expect(result.data[0].uniqueSkills).toBe(2); // Python, Java

        // Query for Marketing department's unique skills
        const result2 = exec(`
          MATCH (d:Department {name: 'Marketing'})-[:HAS_EMPLOYEE]->(e:Employee)
          RETURN count(DISTINCT e.skill) as uniqueSkills
        `);

        expect(result2.data).toHaveLength(1);
        expect(result2.data[0].uniqueSkills).toBe(1); // SEO
      });
    });

    describe("Anonymous nodes in patterns", () => {
      it("matches relationship with anonymous source and target", () => {
        // Pattern: MATCH ()-[r:KNOWS]->() RETURN r
        exec("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})");
        exec("CREATE (c:Person {name: 'Charlie'})-[:KNOWS]->(d:Person {name: 'Diana'})");

        const result = exec(`
          MATCH ()-[r:KNOWS]->()
          RETURN r
        `);

        expect(result.data).toHaveLength(2);
      });

      it("matches relationship with anonymous target only", () => {
        // Pattern: MATCH (a:Person)-[r:KNOWS]->() RETURN a, r
        exec("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})");
        exec("CREATE (a:Person {name: 'Alice'})-[:WORKS_AT]->(c:Company {name: 'Acme'})");

        const result = exec(`
          MATCH (p:Person {name: 'Alice'})-[r:KNOWS]->()
          RETURN p.name as name, r
        `);

        expect(result.data).toHaveLength(1);
        expect(result.data[0].name).toBe("Alice");
      });

      it("matches relationship with anonymous source only", () => {
        // Pattern: MATCH ()-[r:WORKS_AT]->(c:Company) RETURN c, r
        exec("CREATE (a:Person {name: 'Alice'})-[:WORKS_AT]->(c:Company {name: 'Acme'})");
        exec("CREATE (b:Person {name: 'Bob'})-[:WORKS_AT]->(c:Company {name: 'Acme'})");

        const result = exec(`
          MATCH ()-[r:WORKS_AT]->(c:Company {name: 'Acme'})
          RETURN c.name as company, count(r) as employeeCount
        `);

        expect(result.data).toHaveLength(1);
        expect(result.data[0].company).toBe("Acme");
        expect(result.data[0].employeeCount).toBe(2);
      });

      it("matches all relationships with anonymous nodes", () => {
        // Pattern: MATCH ()-[r]->() RETURN count(r)
        exec("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})");
        exec("CREATE (a:Person {name: 'Alice'})-[:WORKS_AT]->(c:Company {name: 'Acme'})");

        // Use id(a) to link properly
        const alice = exec("MATCH (p:Person {name: 'Alice'}) RETURN id(p) as aid").data[0];
        const bob = exec("MATCH (p:Person {name: 'Bob'}) RETURN id(p) as bid").data[0];
        
        // Query all relationships
        const result = exec(`
          MATCH ()-[r]->()
          RETURN count(r) as totalRels
        `);

        expect(result.data).toHaveLength(1);
        expect(result.data[0].totalRels).toBeGreaterThanOrEqual(2);
      });
    });

    describe("Label predicates on anonymous nodes", () => {
      it("matches pattern with labeled anonymous nodes", () => {
        // Pattern: MATCH (:Person)-[r:WORKS_AT]->(:Company) RETURN r
        exec("CREATE (a:Person {name: 'Alice'})-[:WORKS_AT]->(c:Company {name: 'Acme'})");
        exec("CREATE (b:Person {name: 'Bob'})-[:WORKS_AT]->(d:Company {name: 'BigCorp'})");
        exec("CREATE (x:Robot {name: 'R2D2'})-[:WORKS_AT]->(c:Company {name: 'Acme'})");

        const result = exec(`
          MATCH (:Person)-[r:WORKS_AT]->(:Company)
          RETURN count(r) as count
        `);

        expect(result.data).toHaveLength(1);
        expect(result.data[0].count).toBe(2); // Only Person->Company, not Robot->Company
      });

      it("matches mixed named and anonymous nodes with labels", () => {
        // Pattern: MATCH (p:Person)-[:WORKS_AT]->(:Company) RETURN p.name
        exec("CREATE (a:Person {name: 'Alice'})-[:WORKS_AT]->(c:Company {name: 'Acme'})");
        exec("CREATE (b:Person {name: 'Bob'})-[:KNOWS]->(c:Person {name: 'Charlie'})");

        const result = exec(`
          MATCH (p:Person)-[:WORKS_AT]->(:Company)
          RETURN p.name as name
        `);

        expect(result.data).toHaveLength(1);
        expect(result.data[0].name).toBe("Alice");
      });

      it("matches anonymous source with label predicate", () => {
        // Pattern: MATCH (:Department)-[:CONTAINS]->(e:Employee) RETURN e
        exec("CREATE (d:Department {name: 'Engineering'})-[:CONTAINS]->(e:Employee {name: 'Alice'})");
        exec("CREATE (d:Department {name: 'Marketing'})-[:CONTAINS]->(e:Employee {name: 'Bob'})");
        exec("CREATE (p:Project {name: 'Secret'})-[:CONTAINS]->(e:Employee {name: 'Charlie'})");

        const result = exec(`
          MATCH (:Department)-[:CONTAINS]->(e:Employee)
          RETURN e.name as name
          ORDER BY name
        `);

        expect(result.data).toHaveLength(2);
        expect(result.data[0].name).toBe("Alice");
        expect(result.data[1].name).toBe("Bob");
      });
    });

    describe("MERGE with ON CREATE SET and RETURN comparison", () => {
      it("handles MERGE with ON CREATE SET followed by RETURN with equality comparison", () => {
        // This pattern is used to check if a node was created or matched
        // Pattern from user: MATCH (u:BF_User {id: $userId})
        //                    MERGE (u)-[:BF_LEARNS]->(l:BF_Language {language: $language})
        //                    ON CREATE SET l.proficiency = $proficiency, l.created_at = $createdAt
        //                    RETURN l.created_at = $createdAt as created
        exec("CREATE (u:BF_User {id: 'user-1'})");

        const createdAt = "2024-01-15T10:00:00Z";
        
        // First call should create the language node
        const result1 = exec(`
          MATCH (u:BF_User {id: $userId})
          MERGE (u)-[:BF_LEARNS]->(l:BF_Language {language: $language})
          ON CREATE SET l.proficiency = $proficiency,
                        l.created_at = $createdAt
          RETURN l.created_at = $createdAt as created
        `, {
          userId: "user-1",
          language: "Spanish",
          proficiency: "beginner",
          createdAt: createdAt,
        });

        expect(result1.data).toHaveLength(1);
        expect(result1.data[0].created).toBe(true); // Was created, timestamps match

        // Second call should match existing node (ON CREATE SET won't run)
        const result2 = exec(`
          MATCH (u:BF_User {id: $userId})
          MERGE (u)-[:BF_LEARNS]->(l:BF_Language {language: $language})
          ON CREATE SET l.proficiency = $proficiency,
                        l.created_at = $createdAt
          RETURN l.created_at = $createdAt as created
        `, {
          userId: "user-1",
          language: "Spanish",
          proficiency: "intermediate",
          createdAt: "2024-01-20T10:00:00Z", // Different timestamp
        });

        expect(result2.data).toHaveLength(1);
        // Should be false because the node was matched, not created
        // so created_at still has the original value
        expect(result2.data[0].created).toBe(false);
      });
    });
  });

  describe("List Concatenation", () => {
    it("concatenates two literal lists with + operator", () => {
      // Pattern: RETURN [1, 2] + [3, 4] AS combined
      const result = exec("RETURN [1, 2] + [3, 4] AS combined");

      expect(result.data).toHaveLength(1);
      expect(result.data[0].combined).toEqual([1, 2, 3, 4]);
    });

    it("concatenates list with single element", () => {
      // Pattern: RETURN [1, 2, 3] + [4] AS extended
      const result = exec("RETURN [1, 2, 3] + [4] AS extended");

      expect(result.data).toHaveLength(1);
      expect(result.data[0].extended).toEqual([1, 2, 3, 4]);
    });

    it("concatenates empty lists", () => {
      // Pattern: RETURN [] + [] AS empty
      const result = exec("RETURN [] + [] AS empty");

      expect(result.data).toHaveLength(1);
      expect(result.data[0].empty).toEqual([]);
    });

    it("concatenates list with empty list", () => {
      // Pattern: RETURN [1, 2] + [] AS unchanged
      const result = exec("RETURN [1, 2] + [] AS unchanged");

      expect(result.data).toHaveLength(1);
      expect(result.data[0].unchanged).toEqual([1, 2]);
    });

    it("concatenates property list with literal list", () => {
      // Pattern: RETURN n.tags + ['new'] AS allTags
      exec("CREATE (n:Item {name: 'Test', tags: ['a', 'b']})");

      const result = exec(`
        MATCH (n:Item {name: 'Test'})
        RETURN n.tags + ['new'] AS allTags
      `);

      expect(result.data).toHaveLength(1);
      expect(result.data[0].allTags).toEqual(["a", "b", "new"]);
    });

    it("concatenates two property lists", () => {
      exec("CREATE (n:Item {list1: [1, 2], list2: [3, 4]})");

      const result = exec(`
        MATCH (n:Item)
        RETURN n.list1 + n.list2 AS combined
      `);

      expect(result.data).toHaveLength(1);
      expect(result.data[0].combined).toEqual([1, 2, 3, 4]);
    });

    it("concatenates string lists", () => {
      const result = exec("RETURN ['a', 'b'] + ['c', 'd'] AS letters");

      expect(result.data).toHaveLength(1);
      expect(result.data[0].letters).toEqual(["a", "b", "c", "d"]);
    });

    it("concatenates mixed type lists", () => {
      const result = exec("RETURN [1, 'two'] + [3, 'four'] AS mixed");

      expect(result.data).toHaveLength(1);
      expect(result.data[0].mixed).toEqual([1, "two", 3, "four"]);
    });

    it("chains multiple list concatenations", () => {
      // Pattern: RETURN [1] + [2] + [3] AS chain
      const result = exec("RETURN [1] + [2] + [3] AS chain");

      expect(result.data).toHaveLength(1);
      expect(result.data[0].chain).toEqual([1, 2, 3]);
    });
  });
});
