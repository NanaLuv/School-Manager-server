const pool = require("../db");
const XLSX = require("xlsx");
const path = require("path");
const fs = require("fs");
const { jsPDF } = require("jspdf");
const { autoTable } = require("jspdf-autotable");

// CACHE SETUP
const NodeCache = require("node-cache");
const cache = new NodeCache({ stdTTL: 300 }); // 5 minutes default

// Function to clear relevant caches based on operation
const clearRelevantCaches = (operation, data = {}) => {
  console.log(`🗑️ [CACHE CLEARING] Operation: ${operation} ,data:${data}`);
  if (data && typeof data !== "object") {
    data = {};
  }
  switch (operation) {
    case "UPDATE_SCHOOL_SETTINGS":
      cache.del("school_settings_pdf");
      break;

    case "UPDATE_ACADEMIC_YEAR":
    case "SET_CURRENT_YEAR":
      cache.del("current_academic_year");
      cache.del("current_terms_list");
      cache.del("classes_list_with_counts");
      break;


    case "UPDATE_CLASS":
      cache.del("classes_list_with_counts");
      cache.del("students_by_class");
      break;

    case "UPDATE_STUDENT":
      cache.del("students_list");
      cache.del("classes_list_with_counts");
      break;

    case "PROCESS_PAYMENT":
      const { student_id, academic_year_id, term_id } = data;
      cache.del(`term_balance_${student_id}_${academic_year_id}_${term_id}`);
      cache.del("student_bills_list");
      break;

    case "UPDATE_TERM_BILL":
      const { studentId, academicYearId, termId } = data;
      cache.del(`term_balance_${studentId}_${academicYearId}_${termId}`);
      break;

    case "UPDATE_TEACHER":
      cache.del("teachers_list");
      cache.del("available_teachers");
      break;

    case "UPDATE_SUBJECT":
      cache.del("subjects_list");
      break;

    case "UPDATE_FEE_CATEGORY":
      cache.del("fee_categories_list");
      break;

    case "UPDATE_BILL_TEMPLATE":
      cache.del("bill_templates_list");
      break;

    case "UPDATE_TERM":
    case "CREATE_TERM":
    case "DELETE_TERM":
      // Clear the current terms list cache
      cache.del("current_terms_list");

      // Also clear any cached terms for specific academic years
      if (data.academic_year_id) {
        cache.del(`terms_by_year_${data.academic_year_id}`);
      } else {
        // If we don't know which year, clear all term caches
        const keys = cache.keys();
        keys.forEach((key) => {
          if (key.startsWith("terms_by_year_")) {
            cache.del(key);
          }
        });
      }
      break;

    case "DELETE_ACADEMIC_YEAR":
    case "SET_CURRENT_YEAR":
      // Clear all term caches when academic years change
      const keys = cache.keys();
      keys.forEach((key) => {
        if (key.startsWith("terms_by_year_") || key === "current_terms_list") {
          cache.del(key);
        }
      });
      break;

    case "CLEAR_ALL":
      cache.flushAll();
      console.log("🗑️ [CACHE CLEARED] All cache cleared");
      break;
  }

  console.log(`✅ [CACHE CLEARED] ${operation} complete`);
};

// GET /api/users - Get all users (for received_by dropdown)
const getUsers = async (req, res) => {
  try {
    const [users] = await pool.query(`
      SELECT 
        u.id,
        u.username,
        u.email,
        u.created_at,
        u.is_active,
        r.role_name
      FROM users u
      LEFT JOIN roles r ON u.role_id = r.id
      ORDER BY u.username
    `);
    res.json(users);
  } catch (error) {
    console.error("Error fetching users:", error);
    res.status(500).json({ error: "Failed to fetch users" });
  }
};

// GET /api/subjects - Get all subjects
const getSubjects = async (req, res) => {
  try {
    const [subjects] = await pool.query(`
      SELECT * FROM subjects 
      ORDER BY created_at DESC
    `);
    res.json(subjects);
  } catch (error) {
    console.error("Error fetching subjects:", error);
    res.status(500).json({ error: "Failed to fetch subjects" });
  }
};

// POST /api/subjects - Create new subject
const createNewSubject = async (req, res) => {
  try {
    const { subject_code, subject_name, description } = req.body;

    // Check if subject code already exists
    const [existing] = await pool.query(
      "SELECT id FROM subjects WHERE subject_code = ?",
      [subject_code],
    );

    if (existing.length > 0) {
      return res.status(400).json({ error: "Subject code already exists" });
    }

    // Insert new subject
    const [result] = await pool.query(
      "INSERT INTO subjects (subject_code, subject_name, description) VALUES (?, ?, ?)",
      [subject_code, subject_name, description],
    );
    clearRelevantCaches("UPDATE_SUBJECT");

    // Return the created subject
    const [newSubject] = await pool.query(
      "SELECT * FROM subjects WHERE id = ?",
      [result.insertId],
    );

    res.status(201).json(newSubject[0]);
  } catch (error) {
    clearRelevantCaches("UPDATE_SUBJECT");
    console.error("Error creating subject:", error);
    res.status(500).json({ error: "Failed to create subject" });
  }
};

// PUT /api/subjects/:id - Update subject
const updateSubjects = async (req, res) => {
  try {
    const { subject_code, subject_name, description } = req.body;

    // Check if subject exists
    const [existing] = await pool.query(
      "SELECT id FROM subjects WHERE id = ?",
      [req.params.id],
    );

    if (existing.length === 0) {
      return res.status(404).json({ error: "Subject not found" });
    }

    // Check if subject code is taken by another subject
    const [codeCheck] = await pool.query(
      "SELECT id FROM subjects WHERE subject_code = ? AND id != ?",
      [subject_code, req.params.id],
    );

    if (codeCheck.length > 0) {
      return res.status(400).json({ error: "Subject code already exists" });
    }

    // Update subject
    await pool.query(
      "UPDATE subjects SET subject_code = ?, subject_name = ?, description = ? WHERE id = ?",
      [subject_code, subject_name, description, req.params.id],
    );
    clearRelevantCaches("UPDATE_SUBJECT");

    // Return updated subject
    const [updatedSubject] = await pool.query(
      "SELECT * FROM subjects WHERE id = ?",
      [req.params.id],
    );

    res.json(updatedSubject[0]);
  } catch (error) {
    clearRelevantCaches("UPDATE_SUBJECT");
    console.error("Error updating subject:", error);
    res.status(500).json({ error: "Failed to update subject" });
  }
};

//delete subject
const deleteSubject = async (req, res) => {
  try {
    const { id } = req.params;
    const [response] = await pool.execute("DELETE FROM subjects WHERE id = ?", [
      id,
    ]);
    res.status(201).json({ msg: "subject deleted successfully", response });
  } catch (error) {
    console.error("Error deleting subject:", error);
  }
};

// GET /api/teachers - Get all teachers
const getTeachers = async (req, res) => {
  try {
    const [teachers] = await pool.query(`
      SELECT t.*, u.email, u.is_active 
      FROM teachers t
      LEFT JOIN users u ON t.user_id = u.id
      ORDER BY t.first_name, t.last_name
    `);
    res.json(teachers);
  } catch (error) {
    console.error("Error fetching teachers:", error);
    res.status(500).json({ error: "Failed to fetch teachers" });
  }
};

// POST /api/teachers - Create new teacher
const createTeacher = async (req, res) => {
  try {
    const {
      employee_id,
      first_name,
      last_name,
      contact_info,
      specialization,
      hire_date,
    } = req.body;

    const [result] = await pool.query(
      "INSERT INTO teachers (employee_id, first_name, last_name, contact_info, specialization, hire_date) VALUES (?, ?, ?, ?, ?, ?)",
      [
        employee_id,
        first_name,
        last_name,
        contact_info,
        specialization,
        hire_date,
      ],
    );
    clearRelevantCaches("UPDATE_TEACHER");

    const [newTeacher] = await pool.query(
      "SELECT * FROM teachers WHERE id = ?",
      [result.insertId],
    );
    res.status(201).json(newTeacher[0]);
  } catch (error) {
    clearRelevantCaches("UPDATE_TEACHER");
    console.error("Error creating teacher:", error);
    res.status(500).json({ error: "Failed to create teacher" });
  }
};

// PUT /api/teachers/:id - Update teachers
const updateTeachers = async (req, res) => {
  try {
    const {
      employee_id,
      first_name,
      last_name,
      contact_info,
      specialization,
      hire_date,
    } = req.body;
    // Check if teacher exists
    const [existing] = await pool.query(
      "SELECT id FROM teachers WHERE id = ?",
      [req.params.id],
    );

    if (existing.length === 0) {
      return res.status(404).json({ error: "teacher not found" });
    }

    // Check if teacher code is taken by another teacher
    const [employeeId] = await pool.query(
      "SELECT id FROM teachers WHERE employee_id = ? AND id != ?",
      [employee_id, req.params.id],
    );

    if (employeeId.length > 0) {
      return res.status(400).json({ error: "Employee id already exists" });
    }

    // Update teacher
    await pool.query(
      "UPDATE teachers SET employee_id = ?, first_name = ?, last_name = ?, contact_info = ?, specialization = ?, hire_date = ? WHERE id = ?",
      [
        employee_id,
        first_name,
        last_name,
        contact_info,
        specialization,
        hire_date,
        req.params.id,
      ],
    );
    clearRelevantCaches("UPDATE_TEACHER");

    // Return updated subject
    const [updatedTeacher] = await pool.query(
      "SELECT * FROM teachers WHERE id = ?",
      [req.params.id],
    );

    res.json(updatedTeacher[0]);
  } catch (error) {
    clearRelevantCaches("UPDATE_TEACHER");
    console.error("Error updating teacher:", error);
    res.status(500).json({ error: "Failed to update teacher" });
  }
};

const deleteTeacher = async (req, res) => {
  try {
    const { id } = req.params;
    const [response] = await pool.execute("DELETE FROM teachers WHERE id = ?", [
      id,
    ]);
    res.status(201).json({ msg: "teacher deleted successfully", response });
  } catch (error) {
    console.error("Error deleting teacher:", error);
    res.status(500).json({ error: "Failed to delete teacher" });
  }
};

// GET /api/academic-years - Get all academic years with term counts
const getAcademicYears = async (req, res) => {
  try {
    const [years] = await pool.query(`
      SELECT ay.*, 
             COUNT(t.id) as term_count
      FROM academic_years ay
      LEFT JOIN terms t ON ay.id = t.academic_year_id
      GROUP BY ay.id
      ORDER BY ay.start_date DESC
    `);
    res.json(years);
  } catch (error) {
    console.error("Error fetching academic years:", error);
    res.status(500).json({ error: "Failed to fetch academic years" });
  }
};
// Update the getAcademicYears function to support pagination
const getAcademicYearsPaginated = async (req, res) => {
  try {
    const {
      page = 1,
      limit = 10,
      search = "",
      status = "all", // all, current, past, upcoming
      sort_by = "start_date",
      sort_order = "desc",
    } = req.query;

    const pageNum = parseInt(page);
    const limitNum = parseInt(limit);
    const offset = (pageNum - 1) * limitNum;

    // Build WHERE conditions
    let whereConditions = ["1=1"];
    let queryParams = [];

    // Search filter
    if (search) {
      whereConditions.push("(ay.year_label LIKE ?)");
      queryParams.push(`%${search}%`);
    }

    // Status filter
    if (status === "current") {
      whereConditions.push("ay.is_current = TRUE");
    } else if (status === "past") {
      whereConditions.push("ay.end_date < CURDATE()");
    } else if (status === "upcoming") {
      whereConditions.push("ay.start_date > CURDATE()");
    } else if (status === "active") {
      whereConditions.push(
        "ay.start_date <= CURDATE() AND ay.end_date >= CURDATE()",
      );
    }

    // Validate sort order
    const validSortFields = ["year_label", "start_date", "end_date"];
    const validSortOrders = ["asc", "desc"];

    const sortField = validSortFields.includes(sort_by)
      ? sort_by
      : "start_date";
    const sortOrder = validSortOrders.includes(sort_order.toLowerCase())
      ? sort_order.toUpperCase()
      : "DESC";

    // Get total count first
    const [countResult] = await pool.query(
      `SELECT COUNT(*) as total
       FROM academic_years ay
       WHERE ${whereConditions.join(" AND ")}`,
      queryParams,
    );

    const total = countResult[0].total;
    const totalPages = Math.ceil(total / limitNum);

    // Get paginated data with term counts
    queryParams.push(limitNum, offset);
    const [years] = await pool.query(
      `SELECT ay.*, 
              COUNT(t.id) as term_count
       FROM academic_years ay
       LEFT JOIN terms t ON ay.id = t.academic_year_id
       WHERE ${whereConditions.join(" AND ")}
       GROUP BY ay.id
       ORDER BY ${sortField} ${sortOrder}
       LIMIT ? OFFSET ?`,
      queryParams,
    );

    res.json({
      years,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total,
        totalPages,
        hasNextPage: pageNum < totalPages,
        hasPrevPage: pageNum > 1,
      },
      filters: {
        search,
        status,
        sort_by: sortField,
        sort_order: sortOrder.toLowerCase(),
      },
    });
  } catch (error) {
    console.error("Error fetching academic years:", error);
    res.status(500).json({ error: "Failed to fetch academic years" });
  }
};

// POST /api/academic-years - Create new academic year
const createAcademicYear = async (req, res) => {
  try {
    const { year_label, start_date, end_date, is_current } = req.body;

    // If setting as current, unset any other current year
    if (is_current) {
      await pool.query(
        "UPDATE academic_years SET is_current = FALSE WHERE is_current = TRUE",
      );
    }

    const [result] = await pool.query(
      "INSERT INTO academic_years (year_label, start_date, end_date, is_current) VALUES (?, ?, ?, ?)",
      [year_label, start_date, end_date, is_current],
    );

    const [newYear] = await pool.query(
      "SELECT * FROM academic_years WHERE id = ?",
      [result.insertId],
    );
    res.status(201).json(newYear[0]);
  } catch (error) {
    console.error("Error creating academic year:", error);
    res.status(500).json({ error: "Failed to create academic year" });
  }
};

// PUT /api/academic-years/:id - Update academic year
const updateAcademicYear = async (req, res) => {
  try {
    const { year_label, start_date, end_date, is_current } = req.body;

    // If setting as current, unset any other current year
    if (is_current) {
      await pool.query(
        "UPDATE academic_years SET is_current = FALSE WHERE is_current = TRUE AND id != ?",
        [req.params.id],
      );
    }

    await pool.query(
      "UPDATE academic_years SET year_label = ?, start_date = ?, end_date = ?, is_current = ? WHERE id = ?",
      [year_label, start_date, end_date, is_current, req.params.id],
    );

    clearRelevantCaches("UPDATE_ACADEMIC_YEAR");

    const [updatedYear] = await pool.query(
      "SELECT * FROM academic_years WHERE id = ?",
      [req.params.id],
    );
    res.json(updatedYear[0]);
  } catch (error) {
    clearRelevantCaches("UPDATE_ACADEMIC_YEAR");
    console.error("Error updating academic year:", error);
    res.status(500).json({ error: "Failed to update academic year" });
  }
};

// PUT /api/academic-years/:id/set-current - Set academic year as current
const setCurrentYear = async (req, res) => {
  try {
    // Unset any other current year
    await pool.query(
      "UPDATE academic_years SET is_current = FALSE WHERE is_current = TRUE",
    );

    // Set this year as current
    await pool.query(
      "UPDATE academic_years SET is_current = TRUE WHERE id = ?",
      [req.params.id],
    );
    clearRelevantCaches("SET_CURRENT_YEAR");

    res.json({ message: "Academic year set as current" });
  } catch (error) {
    clearRelevantCaches("SET_CURRENT_YEAR");
    console.error("Error setting current year:", error);
    res.status(500).json({ error: "Failed to set current year" });
  }
};

// DELETE /api/academic-years/:id - Delete academic year
const deleteAcademicYear = async (req, res) => {
  try {
    const { id } = req.params;

    const [existing] = await pool.query(
      "SELECT id FROM academic_years WHERE id = ?",
      [id],
    );
    if (existing.length === 0) {
      return res.status(404).json({ error: "Academic year not found" });
    }

    await pool.execute("DELETE FROM academic_years WHERE id = ?", [id]);
    res.json({ message: "Academic year deleted successfully" });
  } catch (error) {
    console.error("Error deleting academic year:", error);
    res.status(500).json({ error: "Failed to delete academic year" });
  }
};

// GET /api/terms - Get all terms with academic year info
const getTerms = async (req, res) => {
  try {
    const cacheKey = "current_terms_list";

    // CHECK CACHE FIRST (unless force refresh requested)
    const cachedTerms = cache.get(cacheKey);
    if (cachedTerms && !req.query.force_refresh) {
      console.log("📦 [CACHE HIT] Terms list from cache");
      return res.json(cachedTerms);
    }

    console.log("🔄 [CACHE MISS] Fetching terms from database");

    // Get current academic year
    const [currentYear] = await pool.query(
      "SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1",
    );

    let terms;

    if (currentYear.length > 0) {
      const currentYearId = currentYear[0].id;

      // Get the first three terms for current academic year
      terms = await pool.query(
        `
        SELECT t.*, ay.year_label, ay.is_current as year_is_current,
               CASE 
                 WHEN t.start_date <= CURDATE() AND t.end_date >= CURDATE() THEN 'current'
                 ELSE 'not_current'
               END as term_status
        FROM terms t
        LEFT JOIN academic_years ay ON t.academic_year_id = ay.id
        WHERE t.academic_year_id = ?
        ORDER BY 
          CASE 
            WHEN t.start_date <= CURDATE() AND t.end_date >= CURDATE() THEN 0
            ELSE 1
          END,
          t.start_date
        LIMIT 3
      `,
        [currentYearId],
      );
    } else {
      // Fallback if no current academic year
      terms = await pool.query(`
        SELECT t.*, ay.year_label, ay.is_current as year_is_current,
               CASE 
                 WHEN t.start_date <= CURDATE() AND t.end_date >= CURDATE() THEN 'current'
                 ELSE 'not_current'
               END as term_status
        FROM terms t
        LEFT JOIN academic_years ay ON t.academic_year_id = ay.id
        ORDER BY t.start_date DESC
        LIMIT 3
      `);
    }

    const result = terms[0]; // Return just the data, not the metadata array

    // CACHE FOR 2 MINUTES (120 seconds)
    cache.set(cacheKey, result, 120);
    console.log("💾 [CACHE SAVED] Terms list cached for 2 minutes");

    res.json(result);
  } catch (error) {
    console.error("Error fetching terms:", error);
    res.status(500).json({ error: "Failed to fetch terms" });
  }
};

// GET /api/terms/by-academic-year - Get all terms for a specific academic year
const getTermsByAcademicYear = async (req, res) => {
  try {
    const { academic_year_id } = req.query;

    if (!academic_year_id) {
      return res.status(400).json({ error: "Academic year ID is required" });
    }

    const cacheKey = `terms_by_year_${academic_year_id}`;

    // CHECK CACHE FIRST
    const cachedTerms = cache.get(cacheKey);
    if (cachedTerms && !req.query.force_refresh) {
      console.log(
        `📦 [CACHE HIT] Terms for academic year ${academic_year_id} from cache`,
      );
      return res.json(cachedTerms);
    }

    console.log(
      `🔄 [CACHE MISS] Fetching terms for academic year ${academic_year_id} from database`,
    );

    const [terms] = await pool.query(
      `
      SELECT t.*, ay.year_label, ay.is_current as year_is_current
      FROM terms t
      LEFT JOIN academic_years ay ON t.academic_year_id = ay.id
      WHERE t.academic_year_id = ?
      ORDER BY t.start_date
    `,
      [academic_year_id],
    );

    // CACHE FOR 5 MINUTES (300 seconds)
    cache.set(cacheKey, terms, 300);
    console.log(
      `💾 [CACHE SAVED] Terms for academic year ${academic_year_id} cached for 5 minutes`,
    );

    res.json(terms);
  } catch (error) {
    console.error("Error fetching terms by academic year:", error);
    res.status(500).json({ error: "Failed to fetch terms" });
  }
};

// POST /api/terms - Create new term
const createTerm = async (req, res) => {
  try {
    const { academic_year_id, term_name, start_date, end_date } = req.body;

    const [result] = await pool.query(
      "INSERT INTO terms (academic_year_id, term_name, start_date, end_date) VALUES (?, ?, ?, ?)",
      [academic_year_id, term_name, start_date, end_date],
    );

    clearRelevantCaches("CREATE_TERM", { academic_year_id });

    const [newTerm] = await pool.query(
      `SELECT t.*, ay.year_label 
       FROM terms t 
       LEFT JOIN academic_years ay ON t.academic_year_id = ay.id 
       WHERE t.id = ?`,
      [result.insertId],
    );

    res.status(201).json(newTerm[0]);
  } catch (error) {
    console.error("Error creating term:", error);
    res.status(500).json({ error: "Failed to create term" });
  }
};

// PUT /api/terms/:id - Update term
const updateTerm = async (req, res) => {
  try {
    const { academic_year_id, term_name, start_date, end_date } = req.body;

    await pool.query(
      "UPDATE terms SET academic_year_id = ?, term_name = ?, start_date = ?, end_date = ? WHERE id = ?",
      [academic_year_id, term_name, start_date, end_date, req.params.id],
    );

    const [updatedTerm] = await pool.query(
      `SELECT t.*, ay.year_label 
       FROM terms t 
       LEFT JOIN academic_years ay ON t.academic_year_id = ay.id 
       WHERE t.id = ?`,
      [req.params.id],
    );

    // Clear caches for both old and new academic years
    clearRelevantCaches("UPDATE_TERM", {
      academic_year_id,
    });

    res.json(updatedTerm[0]);
  } catch (error) {
    // Clear caches for both old and new academic years
    clearRelevantCaches("UPDATE_TERM", {
      academic_year_id,
    });
    console.error("Error updating term:", error);
    res.status(500).json({ error: "Failed to update term" });
  }
};

// DELETE /api/terms/:id - Delete term
const deleteTerm = async (req, res) => {
  try {
    const { id } = req.params;

    const [existing] = await pool.query("SELECT id FROM terms WHERE id = ?", [
      id,
    ]);
    if (existing.length === 0) {
      return res.status(404).json({ error: "Term not found" });
    }

    await pool.execute("DELETE FROM terms WHERE id = ?", [id]);
    // Clear caches for this academic year
    clearRelevantCaches("DELETE_TERM", {
      academic_year_id: currentTerm[0]?.academic_year_id,
    });

    res.json({ message: "Term deleted successfully" });
  } catch (error) {
    console.error("Error deleting term:", error);
    res.status(500).json({ error: "Failed to delete term" });
  }
};

// GET /api/classes - Get all classes with student counts
const getClasses = async (req, res) => {
  try {
    const cacheKey = "classes_list_with_counts";

    // CHECK CACHE FIRST (unless force refresh requested)
    const cachedClasses = cache.get(cacheKey);
    if (cachedClasses && !req.query.force_refresh) {
      console.log("📦 [CACHE HIT] Classes list from cache");
      return res.json(cachedClasses);
    }

    console.log("🔄 [CACHE MISS] Fetching classes from database");

    const [classes] = await pool.query(`
      SELECT 
        c.id, 
        c.class_name, 
        c.capacity, 
        c.room_number,
        COUNT(ca.student_id) as current_student_count,
        (SELECT COUNT(*) FROM class_assignments WHERE class_id = c.id AND academic_year_id = (SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1)) as total_current_students,
        CASE 
          WHEN c.capacity IS NULL THEN 'No limit'
          WHEN c.capacity > 0 THEN CONCAT(ROUND((COUNT(ca.student_id) * 100.0 / c.capacity), 1), '% full')
          ELSE 'No capacity set'
        END as capacity_percentage,
        CASE 
          WHEN c.capacity IS NULL THEN 'info'
          WHEN COUNT(ca.student_id) >= c.capacity THEN 'error'
          WHEN COUNT(ca.student_id) >= c.capacity * 0.8 THEN 'warning'
          ELSE 'success'
        END as capacity_status
      FROM classes c
      LEFT JOIN class_assignments ca ON c.id = ca.class_id 
        AND ca.academic_year_id = (SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1)
      GROUP BY c.id, c.class_name, c.capacity, c.room_number
      ORDER BY c.class_name
    `);

    // CACHE FOR 5 MINUTES (300 seconds)
    cache.set(cacheKey, classes, 300);
    console.log("💾 [CACHE SAVED] Classes list cached for 5 minutes");

    res.json(classes);
  } catch (error) {
    console.error("Error fetching classes:", error);
    res.status(500).json({ error: "Failed to fetch classes" });
  }
};

// GET /api/classes - Get all classes with student counts AND PAGINATION
const getClassesPaginated = async (req, res) => {
  try {
    const {
      page = 1,
      limit = 10,
      search = "",
      sort_by = "class_name",
      sort_order = "asc",
      status = "all", // all, full, available, nearly_full
    } = req.query;

    const pageNum = parseInt(page);
    const limitNum = parseInt(limit);
    const offset = (pageNum - 1) * limitNum;

    // Build WHERE conditions
    let whereConditions = ["1=1"];
    let queryParams = [];

    // Search filter
    if (search) {
      whereConditions.push("(c.class_name LIKE ? OR c.room_number LIKE ?)");
      queryParams.push(`%${search}%`, `%${search}%`);
    }

    // Status filter based on capacity
    if (status === "full") {
      whereConditions.push(`
        c.capacity IS NOT NULL AND 
        (SELECT COUNT(*) FROM class_assignments ca2 
         WHERE ca2.class_id = c.id 
         AND ca2.academic_year_id = (SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1)) >= c.capacity
      `);
    } else if (status === "available") {
      whereConditions.push(`
        c.capacity IS NOT NULL AND 
        (SELECT COUNT(*) FROM class_assignments ca2 
         WHERE ca2.class_id = c.id 
         AND ca2.academic_year_id = (SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1)) < c.capacity * 0.8
      `);
    } else if (status === "nearly_full") {
      whereConditions.push(`
        c.capacity IS NOT NULL AND 
        (SELECT COUNT(*) FROM class_assignments ca2 
         WHERE ca2.class_id = c.id 
         AND ca2.academic_year_id = (SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1)) >= c.capacity * 0.8
      `);
    }

    // Validate sort parameters
    const validSortFields = [
      "class_name",
      "room_number",
      "capacity",
      "current_student_count",
    ];
    const validSortOrders = ["asc", "desc"];

    const sortField = validSortFields.includes(sort_by)
      ? sort_by
      : "class_name";
    const sortOrder = validSortOrders.includes(sort_order.toLowerCase())
      ? sort_order.toUpperCase()
      : "ASC";

    // Build ORDER BY clause
    let orderByClause = "";
    switch (sortField) {
      case "class_name":
        orderByClause = "c.class_name";
        break;
      case "room_number":
        orderByClause = "c.room_number";
        break;
      case "capacity":
        orderByClause = "c.capacity";
        break;
      case "current_student_count":
        orderByClause = "current_student_count";
        break;
      default:
        orderByClause = "c.class_name";
    }

    // Get total count first
    const [countResult] = await pool.query(
      `SELECT COUNT(DISTINCT c.id) as total
       FROM classes c
       LEFT JOIN class_assignments ca ON c.id = ca.class_id 
         AND ca.academic_year_id = (SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1)
       WHERE ${whereConditions.join(" AND ")}`,
      queryParams,
    );

    const total = countResult[0].total;
    const totalPages = Math.ceil(total / limitNum);

    // Get paginated data
    const [classes] = await pool.query(
      `
      SELECT 
        c.id, 
        c.class_name, 
        c.capacity, 
        c.room_number,
        COUNT(ca.student_id) as current_student_count,
        (SELECT COUNT(*) FROM class_assignments WHERE class_id = c.id AND academic_year_id = (SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1)) as total_current_students,
        CASE 
          WHEN c.capacity IS NULL THEN 'No limit'
          WHEN c.capacity > 0 THEN CONCAT(ROUND((COUNT(ca.student_id) * 100.0 / c.capacity), 1), '% full')
          ELSE 'No capacity set'
        END as capacity_percentage,
        CASE 
          WHEN c.capacity IS NULL THEN 'info'
          WHEN COUNT(ca.student_id) >= c.capacity THEN 'error'
          WHEN COUNT(ca.student_id) >= c.capacity * 0.8 THEN 'warning'
          ELSE 'success'
        END as capacity_status
      FROM classes c
      LEFT JOIN class_assignments ca ON c.id = ca.class_id 
        AND ca.academic_year_id = (SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1)
      WHERE ${whereConditions.join(" AND ")}
      GROUP BY c.id, c.class_name, c.capacity, c.room_number
      ORDER BY ${orderByClause} ${sortOrder}
      LIMIT ? OFFSET ?
      `,
      [...queryParams, limitNum, offset],
    );

    // Clear cache for paginated queries
    const cacheKey = `classes_list_with_counts_page_${pageNum}_limit_${limitNum}_search_${search}`;
    cache.del(cacheKey);

    res.json({
      classes,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total,
        totalPages,
        hasNextPage: pageNum < totalPages,
        hasPrevPage: pageNum > 1,
      },
      filters: {
        search,
        status,
        sort_by: sortField,
        sort_order: sortOrder.toLowerCase(),
      },
    });
  } catch (error) {
    console.error("Error fetching classes:", error);
    res.status(500).json({ error: "Failed to fetch classes" });
  }
};

// POST /api/classes - Create new class
const createClass = async (req, res) => {
  try {
    const { class_name, capacity, room_number } = req.body;

    // Check if class name already exists
    const [existing] = await pool.query(
      "SELECT id FROM classes WHERE class_name = ?",
      [class_name],
    );

    if (existing.length > 0) {
      return res.status(400).json({ error: "Class name already exists" });
    }

    const [result] = await pool.query(
      "INSERT INTO classes (class_name, capacity, room_number) VALUES (?, ?, ?)",
      [class_name, capacity, room_number],
    );

    clearRelevantCaches("UPDATE_CLASS");

    const [newClass] = await pool.query("SELECT * FROM classes WHERE id = ?", [
      result.insertId,
    ]);
    res.status(201).json(newClass[0]);
  } catch (error) {
    clearRelevantCaches("UPDATE_CLASS");
    console.error("Error creating class:", error);
    res.status(500).json({ error: "Failed to create class" });
  }
};

// PUT /api/classes/:id - Update class
const updateClass = async (req, res) => {
  try {
    const { class_name, capacity, room_number } = req.body;

    // Check if class exists
    const [existing] = await pool.query("SELECT id FROM classes WHERE id = ?", [
      req.params.id,
    ]);

    if (existing.length === 0) {
      return res.status(404).json({ error: "Class not found" });
    }

    // Check if class name is taken by another class
    const [nameCheck] = await pool.query(
      "SELECT id FROM classes WHERE class_name = ? AND id != ?",
      [class_name, req.params.id],
    );

    if (nameCheck.length > 0) {
      return res.status(400).json({ error: "Class name already exists" });
    }

    await pool.query(
      "UPDATE classes SET class_name = ?, capacity = ?, room_number = ? WHERE id = ?",
      [class_name, capacity, room_number, req.params.id],
    );

    clearRelevantCaches("UPDATE_CLASS");

    const [updatedClass] = await pool.query(
      "SELECT * FROM classes WHERE id = ?",
      [req.params.id],
    );
    res.json(updatedClass[0]);
  } catch (error) {
    clearRelevantCaches("UPDATE_CLASS");
    console.error("Error updating class:", error);
    res.status(500).json({ error: "Failed to update class" });
  }
};

// DELETE /api/classes/:id - Delete class
const deleteClass = async (req, res) => {
  try {
    const { id } = req.params;

    const [existing] = await pool.query("SELECT id FROM classes WHERE id = ?", [
      id,
    ]);
    if (existing.length === 0) {
      return res.status(404).json({ error: "Class not found" });
    }

    await pool.execute("DELETE FROM classes WHERE id = ?", [id]);
    res.json({ message: "Class deleted successfully" });
  } catch (error) {
    console.error("Error deleting class:", error);
    res.status(500).json({ error: "Failed to delete class" });
  }
};

// controllers/classTeacherController.js

// GET /api/class-teachers - Get all class teacher assignments
const getClassTeachers = async (req, res) => {
  try {
    const [classTeachers] = await pool.query(`
      SELECT 
        ct.*,
        c.class_name,
        c.room_number,
        t.first_name as teacher_first_name,
        t.last_name as teacher_last_name,
        t.employee_id,
        ay.year_label as academic_year
      FROM class_teachers ct
      INNER JOIN classes c ON ct.class_id = c.id
      INNER JOIN teachers t ON ct.teacher_id = t.id
      INNER JOIN academic_years ay ON ct.academic_year_id = ay.id
      WHERE ay.is_current = TRUE
      ORDER BY c.class_name, ct.is_main_teacher DESC
    `);
    res.json(classTeachers);
  } catch (error) {
    console.error("Error fetching class teachers:", error);
    res.status(500).json({ error: "Failed to fetch class teachers" });
  }
};

// GET /api/class-teachers/:id - Get specific class teacher assignment
const getClassTeacherById = async (req, res) => {
  try {
    const { id } = req.params;

    const [classTeacher] = await pool.query(
      `
      SELECT 
        ct.*,
        c.class_name,
        c.room_number,
        t.first_name as teacher_first_name,
        t.last_name as teacher_last_name,
        t.employee_id,
        t.contact_info,
        t.specialization,
        ay.year_label as academic_year,
        u.email as teacher_email
      FROM class_teachers ct
      INNER JOIN classes c ON ct.class_id = c.id
      INNER JOIN teachers t ON ct.teacher_id = t.id
      INNER JOIN academic_years ay ON ct.academic_year_id = ay.id
      WHERE ct.id = ?
    `,
      [id],
    );

    if (classTeacher.length === 0) {
      return res
        .status(404)
        .json({ error: "Class teacher assignment not found" });
    }

    res.json(classTeacher[0]);
  } catch (error) {
    console.error("Error fetching class teacher:", error);
    res.status(500).json({ error: "Failed to fetch class teacher" });
  }
};

// POST /api/class-teachers - Assign teacher to class
const assignClassTeacher = async (req, res) => {
  const connection = await pool.getConnection();

  try {
    await connection.beginTransaction();

    const { class_id, teacher_id, is_main_teacher } = req.body;

    // Get current academic year
    const [currentYear] = await connection.query(
      "SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1",
    );

    if (currentYear.length === 0) {
      await connection.rollback();
      return res.status(400).json({ error: "No current academic year set" });
    }

    const academicYearId = currentYear[0].id;

    // Check if this class already has a main teacher for this academic year
    if (is_main_teacher) {
      const [existingMainTeacher] = await connection.query(
        "SELECT id FROM class_teachers WHERE class_id = ? AND academic_year_id = ? AND is_main_teacher = TRUE",
        [class_id, academicYearId],
      );

      if (existingMainTeacher.length > 0) {
        await connection.rollback();
        return res.status(400).json({
          error:
            "This class already has a main teacher for the current academic year",
        });
      }
    }

    // Check if this teacher is already assigned to this class for current year
    const [existingAssignment] = await connection.query(
      "SELECT id FROM class_teachers WHERE class_id = ? AND teacher_id = ? AND academic_year_id = ?",
      [class_id, teacher_id, academicYearId],
    );

    if (existingAssignment.length > 0) {
      await connection.rollback();
      return res.status(400).json({
        error:
          "This teacher is already assigned to this class for the current academic year",
      });
    }

    // Create the assignment
    const [result] = await connection.query(
      `INSERT INTO class_teachers (class_id, teacher_id, academic_year_id, is_main_teacher) 
       VALUES (?, ?, ?, ?)`,
      [class_id, teacher_id, academicYearId, is_main_teacher],
    );

    await connection.commit();

    // Return the created assignment with joined data
    const [newAssignment] = await connection.query(
      `
      SELECT 
        ct.*,
        c.class_name,
        c.room_number,
        t.first_name as teacher_first_name,
        t.last_name as teacher_last_name,
        t.employee_id,
        ay.year_label as academic_year
      FROM class_teachers ct
      INNER JOIN classes c ON ct.class_id = c.id
      INNER JOIN teachers t ON ct.teacher_id = t.id
      INNER JOIN academic_years ay ON ct.academic_year_id = ay.id
      WHERE ct.id = ?
    `,
      [result.insertId],
    );

    res.status(201).json(newAssignment[0]);
  } catch (error) {
    await connection.rollback();
    console.error("Error assigning class teacher:", error);
    res.status(500).json({ error: "Failed to assign class teacher" });
  } finally {
    connection.release();
  }
};

// PUT /api/class-teachers/:id - Update class teacher assignment
const updateClassTeacher = async (req, res) => {
  const connection = await pool.getConnection();

  try {
    await connection.beginTransaction();

    const { id } = req.params;
    const { is_main_teacher } = req.body;

    // Get the existing assignment
    const [existingAssignment] = await connection.query(
      "SELECT * FROM class_teachers WHERE id = ?",
      [id],
    );

    if (existingAssignment.length === 0) {
      await connection.rollback();
      return res
        .status(404)
        .json({ error: "Class teacher assignment not found" });
    }

    // If making this the main teacher, check if another main teacher exists
    if (is_main_teacher) {
      const [existingMainTeacher] = await connection.query(
        "SELECT id FROM class_teachers WHERE class_id = ? AND academic_year_id = ? AND is_main_teacher = TRUE AND id != ?",
        [
          existingAssignment[0].class_id,
          existingAssignment[0].academic_year_id,
          id,
        ],
      );

      if (existingMainTeacher.length > 0) {
        await connection.rollback();
        return res.status(400).json({
          error:
            "This class already has a main teacher for the current academic year",
        });
      }
    }

    // Update the assignment
    await connection.query(
      "UPDATE class_teachers SET is_main_teacher = ? WHERE id = ?",
      [is_main_teacher, id],
    );

    await connection.commit();

    // Return the updated assignment
    const [updatedAssignment] = await connection.query(
      `
      SELECT 
        ct.*,
        c.class_name,
        c.room_number,
        t.first_name as teacher_first_name,
        t.last_name as teacher_last_name,
        t.employee_id,
        ay.year_label as academic_year
      FROM class_teachers ct
      INNER JOIN classes c ON ct.class_id = c.id
      INNER JOIN teachers t ON ct.teacher_id = t.id
      INNER JOIN academic_years ay ON ct.academic_year_id = ay.id
      WHERE ct.id = ?
    `,
      [id],
    );

    res.json(updatedAssignment[0]);
  } catch (error) {
    await connection.rollback();
    console.error("Error updating class teacher:", error);
    res.status(500).json({ error: "Failed to update class teacher" });
  } finally {
    connection.release();
  }
};

// DELETE /api/class-teachers/:id - Remove class teacher assignment
const deleteClassTeacher = async (req, res) => {
  try {
    const { id } = req.params;

    const [result] = await pool.query(
      "DELETE FROM class_teachers WHERE id = ?",
      [id],
    );

    if (result.affectedRows === 0) {
      return res
        .status(404)
        .json({ error: "Class teacher assignment not found" });
    }

    res.json({ message: "Class teacher assignment removed successfully" });
  } catch (error) {
    console.error("Error deleting class teacher:", error);
    res.status(500).json({ error: "Failed to delete class teacher" });
  }
};

// GET /api/teachers/available - Get available teachers (not assigned to any class for current year)
const getAvailableTeachers = async (req, res) => {
  try {
    const [teachers] = await pool.query(`
      SELECT 
        t.id,
        t.employee_id,
        t.first_name,
        t.last_name,
        t.specialization
      FROM teachers t
      WHERE t.id NOT IN (
        SELECT DISTINCT teacher_id 
        FROM class_teachers 
        WHERE academic_year_id = (SELECT id FROM academic_years WHERE is_current = TRUE)
      )
      ORDER BY t.first_name, t.last_name
    `);
    res.json(teachers);
  } catch (error) {
    console.error("Error fetching available teachers:", error);
    res.status(500).json({ error: "Failed to fetch available teachers" });
  }
};

// GET /api/classes/:id/students - Get specific class with student details
const getClassWithStudents = async (req, res) => {
  try {
    const { id } = req.params;

    // First, get the class basic information with student count (ALL students - active and inactive)
    const [classData] = await pool.query(
      `
      SELECT 
        c.*,
        COUNT(ca.student_id) as current_student_count,
        (SELECT COUNT(*) FROM class_assignments WHERE class_id = c.id AND academic_year_id = (SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1)) as total_current_students,
        (SELECT COUNT(*) FROM class_assignments ca2 
         INNER JOIN students s ON ca2.student_id = s.id 
         WHERE ca2.class_id = c.id 
         AND ca2.academic_year_id = (SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1)
         AND (s.is_active IS NULL OR s.is_active = TRUE)) as active_student_count,
        CASE 
          WHEN c.capacity IS NULL THEN 'No limit'
          WHEN c.capacity > 0 THEN CONCAT(ROUND((COUNT(ca.student_id) * 100.0 / c.capacity), 1), '% full')
          ELSE 'No capacity set'
        END as capacity_percentage,
        CASE 
          WHEN c.capacity IS NULL THEN 'info'
          WHEN COUNT(ca.student_id) >= c.capacity THEN 'error'
          WHEN COUNT(ca.student_id) >= c.capacity * 0.8 THEN 'warning'
          ELSE 'success'
        END as capacity_status
      FROM classes c
      LEFT JOIN class_assignments ca ON c.id = ca.class_id 
        AND ca.academic_year_id = (SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1)
      WHERE c.id = ?
      GROUP BY c.id
    `,
      [id],
    );

    if (classData.length === 0) {
      return res.status(404).json({ error: "Class not found" });
    }

    // Then get only ACTIVE students in this class for current academic year
    const [students] = await pool.query(
      `
      SELECT 
        s.id,
        s.admission_number,
        s.first_name,
        s.last_name,
        s.gender,
        s.date_of_birth,
        s.parent_name,
        s.parent_contact,
        s.address,
        s.enrolled_date,
        s.has_fee_block,
        s.is_active,
        s.photo_filename,
        ca.promotion_status,
        ca.date_assigned
      FROM students s
      INNER JOIN class_assignments ca ON s.id = ca.student_id
      WHERE ca.class_id = ? 
        AND ca.academic_year_id = (SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1)
        AND (s.is_active IS NULL OR s.is_active = TRUE)  
      ORDER BY s.first_name, s.last_name
    `,
      [id],
    );

    // Combine the data
    const response = {
      ...classData[0],
      students: students,
      inactive_student_count:
        classData[0].current_student_count - classData[0].active_student_count,
    };

    res.json(response);
  } catch (error) {
    console.error("Error fetching class with students:", error);
    res.status(500).json({ error: "Failed to fetch class details" });
  }
};

// GET /api/classes/:id/export-students - Export students from specific class
const exportClassStudents = async (req, res) => {
  try {
    const { id } = req.params;

    // Get class information
    const [classData] = await pool.query(
      "SELECT class_name FROM classes WHERE id = ?",
      [id],
    );

    if (classData.length === 0) {
      return res.status(404).json({ error: "Class not found" });
    }

    const className = classData[0].class_name;

    // Get students in this class for current academic year
    const [students] = await pool.query(
      `
      SELECT 
        s.admission_number,
        s.first_name,
        s.last_name,
        s.date_of_birth,
        s.gender,
        s.parent_name,
        s.parent_contact,
        s.address,
        s.enrolled_date,
        s.has_fee_block,
        s.is_active,
        ca.promotion_status,
        ca.date_assigned as class_assignment_date
      FROM students s
      INNER JOIN class_assignments ca ON s.id = ca.student_id
      WHERE ca.class_id = ? 
        AND ca.academic_year_id = (SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1)
      ORDER BY s.first_name, s.last_name
    `,
      [id],
    );

    // Prepare data for Excel
    const excelData = students.map((student) => ({
      "Admission Number": student.admission_number,
      "First Name": student.first_name,
      "Last Name": student.last_name,
      // "Date of Birth": student.date_of_birth,
      Gender: student.gender,
      "Parent Name": student.parent_name,
      "Parent Contact": student.parent_contact,
      Address: student.address,
      "Enrollment Date": student.enrolled_date,
      "Promotion Status": student.promotion_status || "Pending",
      "Class Assignment Date": student.class_assignment_date,
      // "Fee Block": student.has_fee_block ? "Yes" : "No",
      Status: student.is_active === false ? "Inactive" : "Active",
    }));

    // Create workbook and worksheet
    const workbook = XLSX.utils.book_new();
    const worksheet = XLSX.utils.json_to_sheet(excelData);

    // Add worksheet to workbook
    XLSX.utils.book_append_sheet(workbook, worksheet, "Students");

    // Generate buffer
    const buffer = XLSX.write(workbook, { type: "buffer", bookType: "xlsx" });

    // Set headers for file download
    const fileName = `class_${className.replace(/\s+/g, "_")}_students_${
      new Date().toISOString().split("T")[0]
    }.xlsx`;

    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    );
    res.setHeader("Content-Disposition", `attachment; filename=${fileName}`);

    res.send(buffer);
  } catch (error) {
    console.error("Error exporting class students:", error);
    res.status(500).json({ error: "Failed to export class students" });
  }
};

// GET /api/subject-assignments - Get all subject assignments with related data
const getSubjectAssignments = async (req, res) => {
  try {
    const [assignments] = await pool.query(`
      SELECT sa.*, 
             s.subject_name, s.subject_code,
             c.class_name, c.room_number,
             t.first_name as teacher_first_name, t.last_name as teacher_last_name,
             ay.year_label
      FROM subject_assignments sa
      LEFT JOIN subjects s ON sa.subject_id = s.id
      LEFT JOIN classes c ON sa.class_id = c.id
      LEFT JOIN teachers t ON sa.teacher_id = t.id
      LEFT JOIN academic_years ay ON sa.academic_year_id = ay.id
      ORDER BY ay.year_label DESC, c.class_name, s.subject_name
    `);
    res.json(assignments);
  } catch (error) {
    console.error("Error fetching subject assignments:", error);
    res.status(500).json({ error: "Failed to fetch subject assignments" });
  }
};

// POST /api/subject-assignments - Create new subject assignment
const createSubjectAssignment = async (req, res) => {
  try {
    const { subject_id, class_id, teacher_id, academic_year_id } = req.body;

    // Check if assignment already exists
    const [existing] = await pool.query(
      "SELECT id FROM subject_assignments WHERE class_id = ? AND subject_id = ? AND academic_year_id = ?",
      [class_id, subject_id, academic_year_id],
    );

    if (existing.length > 0) {
      return res.status(400).json({
        error:
          "This subject is already assigned to this class for the selected academic year",
      });
    }

    const [result] = await pool.query(
      "INSERT INTO subject_assignments (subject_id, class_id, teacher_id, academic_year_id) VALUES (?, ?, ?, ?)",
      [subject_id, class_id, teacher_id, academic_year_id],
    );

    const [newAssignment] = await pool.query(
      `SELECT sa.*, 
              s.subject_name, s.subject_code,
              c.class_name,
              t.first_name as teacher_first_name, t.last_name as teacher_last_name,
              ay.year_label
       FROM subject_assignments sa
       LEFT JOIN subjects s ON sa.subject_id = s.id
       LEFT JOIN classes c ON sa.class_id = c.id
       LEFT JOIN teachers t ON sa.teacher_id = t.id
       LEFT JOIN academic_years ay ON sa.academic_year_id = ay.id
       WHERE sa.id = ?`,
      [result.insertId],
    );

    res.status(201).json(newAssignment[0]);
  } catch (error) {
    console.error("Error creating subject assignment:", error);
    res.status(500).json({ error: "Failed to create subject assignment" });
  }
};

// PUT /api/subject-assignments/:id - Update subject assignment
const updateSubjectAssignment = async (req, res) => {
  try {
    const { subject_id, class_id, teacher_id, academic_year_id } = req.body;

    // Check if assignment exists
    const [existing] = await pool.query(
      "SELECT id FROM subject_assignments WHERE id = ?",
      [req.params.id],
    );

    if (existing.length === 0) {
      return res.status(404).json({ error: "Subject assignment not found" });
    }

    // Check if new assignment would conflict with existing
    const [conflict] = await pool.query(
      "SELECT id FROM subject_assignments WHERE class_id = ? AND subject_id = ? AND academic_year_id = ? AND id != ?",
      [class_id, subject_id, academic_year_id, req.params.id],
    );

    if (conflict.length > 0) {
      return res.status(400).json({
        error:
          "This subject is already assigned to this class for the selected academic year",
      });
    }

    await pool.query(
      "UPDATE subject_assignments SET subject_id = ?, class_id = ?, teacher_id = ?, academic_year_id = ? WHERE id = ?",
      [subject_id, class_id, teacher_id, academic_year_id, req.params.id],
    );

    const [updatedAssignment] = await pool.query(
      `SELECT sa.*, 
              s.subject_name, s.subject_code,
              c.class_name,
              t.first_name as teacher_first_name, t.last_name as teacher_last_name,
              ay.year_label
       FROM subject_assignments sa
       LEFT JOIN subjects s ON sa.subject_id = s.id
       LEFT JOIN classes c ON sa.class_id = c.id
       LEFT JOIN teachers t ON sa.teacher_id = t.id
       LEFT JOIN academic_years ay ON sa.academic_year_id = ay.id
       WHERE sa.id = ?`,
      [req.params.id],
    );

    res.json(updatedAssignment[0]);
  } catch (error) {
    console.error("Error updating subject assignment:", error);
    res.status(500).json({ error: "Failed to update subject assignment" });
  }
};

// DELETE /api/subject-assignments/:id - Delete subject assignment
const deleteSubjectAssignment = async (req, res) => {
  try {
    const { id } = req.params;

    const [existing] = await pool.query(
      "SELECT id FROM subject_assignments WHERE id = ?",
      [id],
    );
    if (existing.length === 0) {
      return res.status(404).json({ error: "Subject assignment not found" });
    }

    await pool.execute("DELETE FROM subject_assignments WHERE id = ?", [id]);
    res.json({ message: "Subject assignment deleted successfully" });
  } catch (error) {
    console.error("Error deleting subject assignment:", error);
    res.status(500).json({ error: "Failed to delete subject assignment" });
  }
};

// GET /api/academic-years - Get academic years
const getAcademicYearsForSujectAssignment = async (req, res) => {
  try {
    const [years] = await pool.query(
      "SELECT * FROM academic_years ORDER BY start_date DESC",
    );
    res.json(years);
  } catch (error) {
    console.error("Error fetching academic years:", error);
    res.status(500).json({ error: "Failed to fetch academic years" });
  }
};

// GET /api/class-assignments - Get class assignments with filters and pagination
const getClassAssignments = async (req, res) => {
  try {
    const {
      class_id,
      academic_year_id,
      student_id,
      page = 1,
      limit = 10,
      search = "",
      sort_by = "student_name",
      sort_order = "asc",
    } = req.query;

    const pageNum = parseInt(page);
    const limitNum = parseInt(limit);
    const offset = (pageNum - 1) * limitNum;

    // Build WHERE conditions
    let whereConditions = ["1=1"];
    let queryParams = [];

    // Class filter
    if (class_id) {
      whereConditions.push("ca.class_id = ?");
      queryParams.push(class_id);
    }

    // Academic year filter
    if (academic_year_id) {
      whereConditions.push("ca.academic_year_id = ?");
      queryParams.push(academic_year_id);
    }

    // Student filter
    if (student_id) {
      whereConditions.push("ca.student_id = ?");
      queryParams.push(student_id);
    }

    // Search filter
    if (search) {
      whereConditions.push(
        "(s.first_name LIKE ? OR s.last_name LIKE ? OR s.admission_number LIKE ? OR c.class_name LIKE ?)",
      );
      queryParams.push(
        `%${search}%`,
        `%${search}%`,
        `%${search}%`,
        `%${search}%`,
      );
    }

    // Get total count
    const [countResult] = await pool.query(
      `SELECT COUNT(DISTINCT ca.id) as total
       FROM class_assignments ca
       LEFT JOIN students s ON ca.student_id = s.id
       LEFT JOIN classes c ON ca.class_id = c.id
       LEFT JOIN academic_years ay ON ca.academic_year_id = ay.id
       WHERE ${whereConditions.join(" AND ")}`,
      queryParams,
    );

    const total = countResult[0].total;
    const totalPages = Math.ceil(total / limitNum);

    // Validate sort
    const validSortFields = [
      "student_name",
      "class_name",
      "admission_number",
      "year_label",
      "date_assigned",
    ];
    const validSortOrders = ["asc", "desc"];

    const sortField = validSortFields.includes(sort_by)
      ? sort_by
      : "student_name";
    const sortOrder = validSortOrders.includes(sort_order.toLowerCase())
      ? sort_order.toUpperCase()
      : "ASC";

    // Build ORDER BY clause
    let orderByClause = "";
    switch (sortField) {
      case "student_name":
        orderByClause = "s.first_name, s.last_name";
        break;
      case "class_name":
        orderByClause = "c.class_name";
        break;
      case "admission_number":
        orderByClause = "s.admission_number";
        break;
      case "year_label":
        orderByClause = "ay.year_label";
        break;
      case "date_assigned":
        orderByClause = "ca.date_assigned";
        break;
    }

    // Get paginated data
    queryParams.push(limitNum, offset);
    const [assignments] = await pool.query(
      `SELECT 
        ca.*, 
        s.first_name, s.last_name, s.admission_number,
        c.class_name, c.room_number, c.capacity,
        ay.year_label,
        ay.is_current as year_is_current,
        ay.start_date as year_start,
        ay.end_date as year_end
      FROM class_assignments ca
      LEFT JOIN students s ON ca.student_id = s.id
      LEFT JOIN classes c ON ca.class_id = c.id
      LEFT JOIN academic_years ay ON ca.academic_year_id = ay.id
      WHERE ${whereConditions.join(" AND ")}
      ORDER BY ${orderByClause} ${sortOrder}
      LIMIT ? OFFSET ?`,
      queryParams,
    );

    res.json({
      assignments,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total,
        totalPages,
        hasNextPage: pageNum < totalPages,
        hasPrevPage: pageNum > 1,
      },
      filters: {
        class_id,
        academic_year_id,
        search,
        sort_by: sortField,
        sort_order: sortOrder.toLowerCase(),
      },
    });
  } catch (error) {
    console.error("Error fetching class assignments:", error);
    res.status(500).json({ error: "Failed to fetch class assignments" });
  }
};

// POST /api/class-assignments - Create new class assignment
const createClassAssignment = async (req, res) => {
  try {
    const { student_id, class_id, academic_year_id, promotion_status } =
      req.body;

    // Enhanced validation - check if academic year exists
    const [yearCheck] = await pool.query(
      "SELECT id, year_label FROM academic_years WHERE id = ?",
      [academic_year_id],
    );

    if (yearCheck.length === 0) {
      return res
        .status(400)
        .json({ error: "Selected academic year does not exist" });
    }
    // Check if student is already assigned to a class for this academic year
    const [existing] = await pool.query(
      `SELECT ca.id, c.class_name, ay.year_label 
       FROM class_assignments ca
       LEFT JOIN classes c ON ca.class_id = c.id
       LEFT JOIN academic_years ay ON ca.academic_year_id = ay.id
       WHERE ca.student_id = ? AND ca.academic_year_id = ?`,
      [student_id, academic_year_id],
    );

    if (existing.length > 0) {
      const existingAssignment = existing[0];
      return res.status(400).json({
        error: `This student is already enrolled in ${existingAssignment.class_name} for the ${existingAssignment.year_label} academic year. Students can only be in one class per academic year.`,
      });
    }

    // Check class capacity if specified
    const [classInfo] = await pool.query(
      "SELECT capacity, class_name FROM classes WHERE id = ?",
      [class_id],
    );

    if (classInfo.length > 0 && classInfo[0].capacity) {
      const [currentEnrollment] = await pool.query(
        "SELECT COUNT(*) as count FROM class_assignments WHERE class_id = ? AND academic_year_id = ?",
        [class_id, academic_year_id],
      );

      if (currentEnrollment[0].count >= classInfo[0].capacity) {
        return res.status(400).json({
          error: `Class ${classInfo[0].class_name} has reached its capacity of ${classInfo[0].capacity} students for this academic year.`,
        });
      }
    }

    const [result] = await pool.query(
      "INSERT INTO class_assignments (student_id, class_id, academic_year_id, promotion_status, date_assigned) VALUES (?, ?, ?, ?, CURDATE())",
      [student_id, class_id, academic_year_id, promotion_status || "Pending"],
    );
    clearRelevantCaches("UPDATE_CLASS");

    const [newAssignment] = await pool.query(
      `SELECT ca.*, 
              s.first_name, s.last_name, s.admission_number,
              c.class_name, c.room_number,
              ay.year_label
       FROM class_assignments ca
       LEFT JOIN students s ON ca.student_id = s.id
       LEFT JOIN classes c ON ca.class_id = c.id
       LEFT JOIN academic_years ay ON ca.academic_year_id = ay.id
       WHERE ca.id = ?`,
      [result.insertId],
    );

    res.status(201).json(newAssignment[0]);
  } catch (error) {
    console.error("Error creating class assignment:", error);
    res.status(500).json({ error: "Failed to create class assignment" });
  }
};

// POST /api/class-assignments/bulk - Create multiple class assignments at once
const createBulkClassAssignments = async (req, res) => {
  const connection = await pool.getConnection();

  try {
    await connection.beginTransaction();

    const {
      student_ids,
      class_id,
      academic_year_id,
      promotion_status = "Pending",
    } = req.body;

    // Validate required fields
    if (
      !student_ids ||
      !Array.isArray(student_ids) ||
      student_ids.length === 0
    ) {
      await connection.rollback();
      return res.status(400).json({
        error: "Student IDs array is required and must not be empty",
      });
    }

    if (!class_id || !academic_year_id) {
      await connection.rollback();
      return res.status(400).json({
        error: "Class ID and Academic Year ID are required",
      });
    }

    // Validate academic year exists
    const [yearCheck] = await connection.query(
      "SELECT id, year_label FROM academic_years WHERE id = ?",
      [academic_year_id],
    );

    if (yearCheck.length === 0) {
      await connection.rollback();
      return res.status(400).json({
        error: "Selected academic year does not exist",
      });
    }

    // Validate class exists and check capacity
    const [classInfo] = await connection.query(
      "SELECT capacity, class_name FROM classes WHERE id = ?",
      [class_id],
    );

    if (classInfo.length === 0) {
      await connection.rollback();
      return res.status(400).json({
        error: "Selected class does not exist",
      });
    }

    const className = classInfo[0].class_name;
    const classCapacity = classInfo[0].capacity;

    // Check current enrollment for this class and academic year
    const [currentEnrollment] = await connection.query(
      "SELECT COUNT(*) as count FROM class_assignments WHERE class_id = ? AND academic_year_id = ?",
      [class_id, academic_year_id],
    );

    const currentCount = currentEnrollment[0].count;

    // Check capacity
    if (classCapacity && currentCount + student_ids.length > classCapacity) {
      await connection.rollback();
      return res.status(400).json({
        error: `Class ${className} has ${currentCount}/${classCapacity} students. Adding ${student_ids.length} more would exceed capacity.`,
        capacity: classCapacity,
        current: currentCount,
        attempted: student_ids.length,
      });
    }

    const results = {
      success: 0,
      errors: [],
      skipped: 0,
      created: 0,
    };

    // Process each student
    for (const studentId of student_ids) {
      try {
        // Check if student exists and is active
        const [studentCheck] = await connection.query(
          "SELECT id, first_name, last_name, admission_number FROM students WHERE id = ? AND (is_active IS NULL OR is_active = TRUE)",
          [studentId],
        );

        if (studentCheck.length === 0) {
          results.errors.push({
            student_id: studentId,
            error: "Student not found or inactive",
          });
          continue;
        }

        const student = studentCheck[0];

        // Check if student already has a class assignment for this academic year
        const [existingAssignment] = await connection.query(
          `SELECT ca.id, c.class_name, ay.year_label 
           FROM class_assignments ca
           LEFT JOIN classes c ON ca.class_id = c.id
           LEFT JOIN academic_years ay ON ca.academic_year_id = ay.id
           WHERE ca.student_id = ? AND ca.academic_year_id = ?`,
          [studentId, academic_year_id],
        );

        if (existingAssignment.length > 0) {
          const existing = existingAssignment[0];
          results.skipped++;
          results.errors.push({
            student_id: studentId,
            student_name: `${student.first_name} ${student.last_name}`,
            error: `Already enrolled in ${existing.class_name} for ${existing.year_label}`,
          });
          continue;
        }

        // Create the assignment
        await connection.query(
          "INSERT INTO class_assignments (student_id, class_id, academic_year_id, promotion_status, date_assigned) VALUES (?, ?, ?, ?, CURDATE())",
          [studentId, class_id, academic_year_id, promotion_status],
        );

        results.success++;
        results.created++;
      } catch (error) {
        results.errors.push({
          student_id: studentId,
          error: error.message,
        });
      }
    }

    await connection.commit();

    res.status(201).json({
      message: `Bulk enrollment completed. ${results.created} students enrolled, ${results.skipped} skipped.`,
      ...results,
      summary: {
        total_students: student_ids.length,
        successfully_enrolled: results.created,
        skipped: results.skipped,
        errors_count: results.errors.length - results.skipped, // Exclude skipped from error count
      },
    });
  } catch (error) {
    await connection.rollback();
    console.error("Error creating bulk class assignments:", error);
    res.status(500).json({
      error: "Failed to create bulk class assignments",
      details: error.message,
    });
  } finally {
    connection.release();
  }
};

// PUT /api/class-assignments/:id - Update class assignment
const updateClassAssignment = async (req, res) => {
  try {
    const { student_id, class_id, academic_year_id, promotion_status } =
      req.body;

    // Check if assignment exists
    const [existing] = await pool.query(
      "SELECT id FROM class_assignments WHERE id = ?",
      [req.params.id],
    );

    if (existing.length === 0) {
      return res.status(404).json({ error: "Class assignment not found" });
    }

    // Check if new assignment would conflict with existing
    const [conflict] = await pool.query(
      `SELECT ca.id, c.class_name, ay.year_label 
       FROM class_assignments ca
       LEFT JOIN classes c ON ca.class_id = c.id
       LEFT JOIN academic_years ay ON ca.academic_year_id = ay.id
       WHERE ca.student_id = ? AND ca.academic_year_id = ? AND ca.id != ?`,
      [student_id, academic_year_id, req.params.id],
    );

    if (conflict.length > 0) {
      const conflictAssignment = conflict[0];
      return res.status(400).json({
        error: `This student is already enrolled in ${conflictAssignment.class_name} for the ${conflictAssignment.year_label} academic year.`,
      });
    }

    // Check class capacity if specified
    const [classInfo] = await pool.query(
      "SELECT capacity, class_name FROM classes WHERE id = ?",
      [class_id],
    );

    if (classInfo.length > 0 && classInfo[0].capacity) {
      const [currentEnrollment] = await pool.query(
        "SELECT COUNT(*) as count FROM class_assignments WHERE class_id = ? AND academic_year_id = ? AND id != ?",
        [class_id, academic_year_id, req.params.id],
      );

      if (currentEnrollment[0].count >= classInfo[0].capacity) {
        return res.status(400).json({
          error: `Class ${classInfo[0].class_name} has reached its capacity of ${classInfo[0].capacity} students for this academic year.`,
        });
      }
    }

    await pool.query(
      "UPDATE class_assignments SET student_id = ?, class_id = ?, academic_year_id = ?, promotion_status = ? WHERE id = ?",
      [student_id, class_id, academic_year_id, promotion_status, req.params.id],
    );

    const [updatedAssignment] = await pool.query(
      `SELECT ca.*, 
              s.first_name, s.last_name, s.admission_number,
              c.class_name, c.room_number,
              ay.year_label
       FROM class_assignments ca
       LEFT JOIN students s ON ca.student_id = s.id
       LEFT JOIN classes c ON ca.class_id = c.id
       LEFT JOIN academic_years ay ON ca.academic_year_id = ay.id
       WHERE ca.id = ?`,
      [req.params.id],
    );

    res.json(updatedAssignment[0]);
  } catch (error) {
    console.error("Error updating class assignment:", error);
    res.status(500).json({ error: "Failed to update class assignment" });
  }
};

// PUT /api/class-assignments/:id/promote - Promote student to next class (academic year change)
const promoteStudent = async (req, res) => {
  try {
    const { new_class_id, next_academic_year_id } = req.body;

    // Get current assignment
    const [currentAssignment] = await pool.query(
      `SELECT ca.*, s.first_name, s.last_name, c.class_name, ay.year_label, ay.end_date
       FROM class_assignments ca
       LEFT JOIN students s ON ca.student_id = s.id
       LEFT JOIN classes c ON ca.class_id = c.id
       LEFT JOIN academic_years ay ON ca.academic_year_id = ay.id
       WHERE ca.id = ?`,
      [req.params.id],
    );

    if (currentAssignment.length === 0) {
      return res.status(404).json({ error: "Class assignment not found" });
    }

    const assignment = currentAssignment[0];

    let nextYearId = next_academic_year_id;

    // If no next academic year provided, find the next one
    if (!nextYearId) {
      const [nextYear] = await pool.query(
        "SELECT * FROM academic_years WHERE start_date > ? ORDER BY start_date ASC LIMIT 1",
        [assignment.end_date || new Date()],
      );

      if (nextYear.length === 0) {
        return res.status(400).json({
          error:
            "No next academic year found. Please create the next academic year first before promoting students.",
        });
      }
      nextYearId = nextYear[0].id;
    }

    // Verify next academic year exists
    const [yearCheck] = await pool.query(
      "SELECT id, year_label FROM academic_years WHERE id = ?",
      [nextYearId],
    );

    if (yearCheck.length === 0) {
      return res.status(400).json({ error: "Next academic year not found" });
    }

    // Verify new class exists
    const [classCheck] = await pool.query(
      "SELECT class_name FROM classes WHERE id = ?",
      [new_class_id],
    );

    if (classCheck.length === 0) {
      return res.status(400).json({ error: "New class not found" });
    }

    // Update current assignment to promoted
    await pool.query(
      "UPDATE class_assignments SET promotion_status = 'Promoted' WHERE id = ?",
      [req.params.id],
    );

    // Check if student already has assignment for next academic year
    const [existingNext] = await pool.query(
      "SELECT id FROM class_assignments WHERE student_id = ? AND academic_year_id = ?",
      [assignment.student_id, nextYearId],
    );

    if (existingNext.length > 0) {
      return res.status(400).json({
        error: `Student already has a class assignment for the ${yearCheck[0].year_label} academic year. Please remove the existing assignment first.`,
      });
    }

    // Create new assignment for next academic year
    const [result] = await pool.query(
      "INSERT INTO class_assignments (student_id, class_id, academic_year_id, promotion_status, date_assigned) VALUES (?, ?, ?, ?, CURDATE())",
      [assignment.student_id, new_class_id, nextYearId, "Pending"],
    );

    // Create promotion record
    await pool.query(
      `INSERT INTO promotion_decisions 
       (student_id, from_academic_year_id, from_class_id, to_academic_year_id, to_class_id, decision, decision_date) 
       VALUES (?, ?, ?, ?, ?, ?, NOW())`,
      [
        assignment.student_id,
        assignment.academic_year_id,
        assignment.class_id,
        nextYearId,
        new_class_id,
        "Promoted",
      ],
    );

    res.json({
      message: `Student ${assignment.first_name} ${assignment.last_name} promoted successfully from ${assignment.class_name} to ${classCheck[0].class_name} for the ${yearCheck[0].year_label} academic year.`,
      promoted: true,
    });
  } catch (error) {
    console.error("Error promoting student:", error);
    res.status(500).json({ error: "Failed to promote student" });
  }
};

// DELETE /api/class-assignments/:id - Delete class assignment
const deleteClassAssignment = async (req, res) => {
  try {
    const { id } = req.params;

    const [existing] = await pool.query(
      `SELECT ca.*, s.first_name, s.last_name, c.class_name, ay.year_label
       FROM class_assignments ca
       LEFT JOIN students s ON ca.student_id = s.id
       LEFT JOIN classes c ON ca.class_id = c.id
       LEFT JOIN academic_years ay ON ca.academic_year_id = ay.id
       WHERE ca.id = ?`,
      [id],
    );

    if (existing.length === 0) {
      return res.status(404).json({ error: "Class assignment not found" });
    }

    const assignment = existing[0];

    await pool.execute("DELETE FROM class_assignments WHERE id = ?", [id]);

    res.json({
      message: `Student ${assignment.first_name} ${assignment.last_name} removed from ${assignment.class_name} for the ${assignment.year_label} academic year.`,
      deleted: true,
    });
  } catch (error) {
    console.error("Error deleting class assignment:", error);
    res.status(500).json({ error: "Failed to delete class assignment" });
  }
};

// GET /api/students - Get students with optional includeInactive parameter
const getStudents = async (req, res) => {
  try {
    const {
      includeInactive,
      page = 1,
      limit = 20,
      search = "",
      class_id,
      status,
    } = req.query;

    const pageNum = parseInt(page);
    const limitNum = parseInt(limit);
    const offset = (pageNum - 1) * limitNum;

    let whereClause = "WHERE (s.is_active IS NULL OR s.is_active = TRUE)";
    let whereParams = [];
    let orderClause = `
      ORDER BY 
        c.class_name IS NULL, 
        c.class_name, 
        s.first_name, 
        s.last_name
    `;

    // If includeInactive is explicitly true, show all students
    if (includeInactive === "true") {
      whereClause = ""; // No filter, get all students
      orderClause = `
        ORDER BY 
          s.is_active DESC,
          c.class_name IS NULL, 
          c.class_name, 
          s.first_name, 
          s.last_name
      `;
    }

    // Add search filter if provided
    if (search) {
      const searchCondition = whereClause ? "AND" : "WHERE";
      whereClause += whereClause ? ` ${searchCondition} ` : "WHERE ";
      whereClause += `(
        s.first_name LIKE ? OR 
        s.last_name LIKE ? OR 
        s.admission_number LIKE ? OR
        s.parent_name LIKE ?
      )`;
      whereParams.push(
        `%${search}%`,
        `%${search}%`,
        `%${search}%`,
        `%${search}%`,
      );
    }

    // Add class filter if provided
    if (class_id) {
      const classCondition = whereClause ? "AND" : "WHERE";
      whereClause += whereClause ? ` ${classCondition} ` : "WHERE ";
      whereClause += `c.id = ?`;
      whereParams.push(class_id);
    }

    // Add status filter
    if (status) {
      const statusCondition = whereClause ? "AND" : "WHERE";
      whereClause += whereClause ? ` ${statusCondition} ` : "WHERE ";
      if (status === "active") {
        whereClause += `(s.is_active IS NULL OR s.is_active = TRUE)`;
      } else if (status === "inactive") {
        whereClause += `s.is_active = FALSE`;
      } else if (status === "fee_block") {
        whereClause += `s.has_fee_block = TRUE`;
      }
    }

    // Get total count first
    const [countResult] = await pool.query(
      `
      SELECT COUNT(*) as total
      FROM students s
      LEFT JOIN class_assignments ca ON s.id = ca.student_id 
        AND ca.academic_year_id = (SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1)
      LEFT JOIN classes c ON ca.class_id = c.id
      LEFT JOIN academic_years ay ON ca.academic_year_id = ay.id
      ${whereClause}
    `,
      whereParams,
    );

    const total = countResult[0].total;
    const totalPages = Math.ceil(total / limitNum);

    // Get paginated data
    whereParams.push(limitNum, offset);
    const [students] = await pool.query(
      `
      SELECT 
        s.*, 
        ca.class_id, 
        c.class_name,
        c.room_number,
        ca.academic_year_id,
        ay.year_label as academic_year,
        ca.promotion_status,
        ca.date_assigned as class_assignment_date
      FROM students s
      LEFT JOIN class_assignments ca ON s.id = ca.student_id 
        AND ca.academic_year_id = (SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1)
      LEFT JOIN classes c ON ca.class_id = c.id
      LEFT JOIN academic_years ay ON ca.academic_year_id = ay.id
      ${whereClause}
      ${orderClause}
      LIMIT ? OFFSET ?
    `,
      whereParams,
    );

    res.json({
      students,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total,
        totalPages,
        hasNextPage: pageNum < totalPages,
        hasPrevPage: pageNum > 1,
      },
    });
  } catch (error) {
    console.error("Error fetching students:", error);
    res.status(500).json({ error: "Failed to fetch students" });
  }
};

// POST /api/students - Create new student with photo
// createStudent function to handle class assignment
const createStudent = async (req, res) => {
  const connection = await pool.getConnection();

  try {
    await connection.beginTransaction();

    const {
      admission_number,
      first_name,
      last_name,
      date_of_birth,
      gender,
      parent_name,
      parent_contact,
      parent_email,
      address,
      enrolled_date,
      has_fee_block,
      is_active,
      class_id, // Add class_id to the request
    } = req.body;

    // Check if admission number already exists
    const [existing] = await connection.query(
      "SELECT id FROM students WHERE admission_number = ? AND (is_active IS NULL OR is_active = TRUE)",
      [admission_number],
    );

    if (existing.length > 0) {
      if (req.file) {
        fs.unlinkSync(req.file.path);
      }
      await connection.rollback();
      return res.status(400).json({ error: "Admission number already exists" });
    }

    let photo_filename = null;
    if (req.file) {
      photo_filename = req.file.filename;
    }

    // Convert string booleans to actual booleans
    const feeBlockBoolean = has_fee_block === "true" || has_fee_block === true;
    const isActiveBoolean = is_active !== "false";

    // Insert student
    const [studentResult] = await connection.query(
      `INSERT INTO students 
       (admission_number, first_name, last_name, date_of_birth, gender, parent_name, parent_contact, parent_email, address, enrolled_date, has_fee_block, is_active, photo_filename) 
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?)`,
      [
        admission_number,
        first_name,
        last_name,
        date_of_birth,
        gender,
        parent_name,
        parent_contact,
        parent_email,
        address,
        enrolled_date,
        feeBlockBoolean,
        isActiveBoolean,
        photo_filename,
      ],
    );

    const studentId = studentResult.insertId;

    // If class_id is provided, assign student to class for current academic year
    if (class_id) {
      // Get current academic year
      const [currentYear] = await connection.query(
        "SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1",
      );

      if (currentYear.length > 0) {
        const academicYearId = currentYear[0].id;

        await connection.query(
          `INSERT INTO class_assignments 
           (student_id, class_id, academic_year_id, promotion_status) 
           VALUES (?, ?, ?, 'Pending')`,
          [studentId, class_id, academicYearId],
        );
      }
    }

    await connection.commit();

    clearRelevantCaches("UPDATE_STUDENT");

    // Return the created student with class info
    const [newStudent] = await connection.query(
      `
      SELECT s.*, 
             ca.class_id, 
             c.class_name,
             ca.academic_year_id,
             ca.promotion_status
      FROM students s
      LEFT JOIN class_assignments ca ON s.id = ca.student_id 
        AND ca.academic_year_id = (SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1)
      LEFT JOIN classes c ON ca.class_id = c.id
      WHERE s.id = ?
    `,
      [studentId],
    );

    res.status(201).json(newStudent[0]);
  } catch (error) {
    await connection.rollback();
    // Delete uploaded file if error occurs
    if (req.file) {
      fs.unlinkSync(req.file.path);
    }
    console.error("Error creating student:", error);
    res.status(500).json({ error: "Failed to create student" });
  } finally {
    connection.release();
  }
};

// PUT /api/students/:id - Update student with class assignment
const updateStudent = async (req, res) => {
  const connection = await pool.getConnection();

  try {
    await connection.beginTransaction();

    const {
      admission_number,
      first_name,
      last_name,
      date_of_birth,
      gender,
      parent_name,
      parent_contact,
      parent_email,
      address,
      enrolled_date,
      has_fee_block,
      is_active,
      existing_photo,
      class_id, 
    } = req.body;

    // Check if student exists
    const [existing] = await connection.query(
      "SELECT id, photo_filename FROM students WHERE id = ?",
      [req.params.id],
    );

    if (existing.length === 0) {
      if (req.file) {
        fs.unlinkSync(req.file.path);
      }
      await connection.rollback();
      return res.status(404).json({ error: "Student not found" });
    }

    // Check if admission number is taken by another student
    const [admissionCheck] = await connection.query(
      "SELECT id FROM students WHERE admission_number = ? AND id != ? AND (is_active IS NULL OR is_active = TRUE)",
      [admission_number, req.params.id],
    );

    if (admissionCheck.length > 0) {
      if (req.file) {
        fs.unlinkSync(req.file.path);
      }
      await connection.rollback();
      return res.status(400).json({ error: "Admission number already exists" });
    }

    let photo_filename = existing_photo;

    // If new photo uploaded, delete old one and use new
    if (req.file) {
      photo_filename = req.file.filename;

      // Delete old photo file if it exists
      if (existing[0].photo_filename) {
        const oldPhotoPath = path.join(
          __dirname,
          "../uploads/students",
          existing[0].photo_filename,
        );
        if (fs.existsSync(oldPhotoPath)) {
          fs.unlinkSync(oldPhotoPath);
        }
      }
    }

    // Convert string booleans to actual booleans
    const feeBlockBoolean = has_fee_block === "true" || has_fee_block === true;
    const isActiveBoolean = is_active !== "false";

    // Update student basic information
    await connection.query(
      `UPDATE students SET 
       admission_number = ?, first_name = ?, last_name = ?, date_of_birth = ?, gender = ?, 
       parent_name = ?, parent_contact = ?, parent_email = ?, address = ?, enrolled_date = ?, has_fee_block = ?, is_active = ?, photo_filename = ?
       WHERE id = ?`,
      [
        admission_number,
        first_name,
        last_name,
        date_of_birth,
        gender,
        parent_name,
        parent_contact,
        parent_email,
        address,
        enrolled_date,
        feeBlockBoolean,
        isActiveBoolean,
        photo_filename,
        req.params.id,
      ],
    );

    // Handle class assignment
    const studentId = req.params.id;

    // Get current academic year
    const [currentYear] = await connection.query(
      "SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1",
    );

    if (currentYear.length > 0) {
      const academicYearId = currentYear[0].id;

      // Check if student already has a class assignment for current year
      const [existingAssignment] = await connection.query(
        "SELECT id, class_id FROM class_assignments WHERE student_id = ? AND academic_year_id = ?",
        [studentId, academicYearId],
      );

      if (class_id) {
        // If class_id is provided, update or create class assignment
        if (existingAssignment.length > 0) {
          // Update existing assignment
          await connection.query(
            "UPDATE class_assignments SET class_id = ? WHERE student_id = ? AND academic_year_id = ?",
            [class_id, studentId, academicYearId],
          );
        } else {
          // Create new assignment
          await connection.query(
            `INSERT INTO class_assignments 
             (student_id, class_id, academic_year_id, promotion_status) 
             VALUES (?, ?, ?, 'Pending')`,
            [studentId, class_id, academicYearId],
          );
        }
      } else {
        // If class_id is empty, remove existing assignment if it exists
        if (existingAssignment.length > 0) {
          await connection.query(
            "DELETE FROM class_assignments WHERE student_id = ? AND academic_year_id = ?",
            [studentId, academicYearId],
          );
        }
      }
    }

    await connection.commit();
    clearRelevantCaches("UPDATE_STUDENT");

    // Return the updated student with class info
    const [updatedStudent] = await connection.query(
      `
      SELECT s.*, 
             ca.class_id, 
             c.class_name,
             ca.academic_year_id,
             ca.promotion_status
      FROM students s
      LEFT JOIN class_assignments ca ON s.id = ca.student_id 
        AND ca.academic_year_id = (SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1)
      LEFT JOIN classes c ON ca.class_id = c.id
      WHERE s.id = ?
    `,
      [studentId],
    );

    res.json(updatedStudent[0]);
  } catch (error) {
    await connection.rollback();
    if (req.file) {
      fs.unlinkSync(req.file.path);
    }
    console.error("Error updating student:", error);
    res.status(500).json({ error: "Failed to update student" });
  } finally {
    connection.release();
  }
};

// PUT /api/students/:id/deactivate - Deactivate student
const deactivateStudent = async (req, res) => {
  try {
    const { id } = req.params;

    const [existing] = await pool.query(
      "SELECT id FROM students WHERE id = ?",
      [id],
    );
    if (existing.length === 0) {
      return res.status(404).json({ error: "Student not found" });
    }

    await pool.query("UPDATE students SET is_active = FALSE WHERE id = ?", [
      id,
    ]);

    res.json({
      message: "Student deactivated successfully",
      deactivated: true,
    });
  } catch (error) {
    console.error("Error deactivating student:", error);
    res.status(500).json({ error: "Failed to deactivate student" });
  }
};

// PUT /api/students/:id/activate - Activate student
const activateStudent = async (req, res) => {
  try {
    const { id } = req.params;

    const [existing] = await pool.query(
      "SELECT id FROM students WHERE id = ?",
      [id],
    );
    if (existing.length === 0) {
      return res.status(404).json({ error: "Student not found" });
    }

    await pool.query("UPDATE students SET is_active = TRUE WHERE id = ?", [id]);

    res.json({
      message: "Student activated successfully",
      activated: true,
    });
  } catch (error) {
    console.error("Error activating student:", error);
    res.status(500).json({ error: "Failed to activate student" });
  }
};

// POST /api/students/import - Import students
const importStudents = async (req, res) => {
  const connection = await pool.getConnection();

  try {
    await connection.beginTransaction();

    const { students } = req.body;
    const results = {
      success: 0,
      errors: [],
    };

    // Get current academic year
    const [currentYear] = await connection.query(
      "SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1",
    );

    if (currentYear.length === 0) {
      await connection.rollback();
      return res.status(400).json({ error: "No current academic year set" });
    }

    const academicYearId = currentYear[0].id;

    for (const studentData of students) {
      try {
        const {
          admission_number,
          first_name,
          last_name,
          date_of_birth,
          gender,
          parent_name,
          parent_contact,
          parent_email,
          address,
          enrolled_date,
          class_name, // Add class_name from Excel
        } = studentData;

        // Validate required fields
        if (
          !admission_number ||
          !first_name ||
          !last_name ||
          !date_of_birth ||
          !gender ||
          !parent_name ||
          !parent_contact ||
          !parent_email
        ) {
          results.errors.push({
            row: studentData._row,
            error: "Missing required fields",
            data: studentData,
          });
          continue;
        }

        // Check if admission number exists
        const [existing] = await connection.query(
          "SELECT id FROM students WHERE admission_number = ? AND (is_active IS NULL OR is_active = TRUE)",
          [admission_number],
        );

        if (existing.length > 0) {
          results.errors.push({
            row: studentData._row,
            error: `Admission number ${admission_number} already exists`,
            data: studentData,
          });
          continue;
        }

        // Insert student
        const [result] = await connection.query(
          `INSERT INTO students 
           (admission_number, first_name, last_name, date_of_birth, gender, parent_name, parent_contact, parent_email, address, enrolled_date, is_active) 
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)`,
          [
            admission_number,
            first_name,
            last_name,
            date_of_birth,
            gender,
            parent_name,
            parent_contact,
            parent_email,
            address,
            enrolled_date || new Date().toISOString().split("T")[0],
          ],
        );

        const studentId = result.insertId;

        // Handle class assignment if class_name is provided
        if (class_name) {
          // Find class by name
          const [classResult] = await connection.query(
            "SELECT id FROM classes WHERE class_name = ?",
            [class_name],
          );

          if (classResult.length > 0) {
            const classId = classResult[0].id;

            // Assign student to class
            await connection.query(
              `INSERT INTO class_assignments 
               (student_id, class_id, academic_year_id, promotion_status) 
               VALUES (?, ?, ?, 'Pending')`,
              [studentId, classId, academicYearId],
            );
          } else {
            // Class not found - log warning but don't fail the import
            results.errors.push({
              row: studentData._row,
              error: `Class '${class_name}' not found - student imported without class assignment`,
              data: studentData,
            });
          }
        }

        results.success++;
      } catch (error) {
        results.errors.push({
          row: studentData._row,
          error: error.message,
          data: studentData,
        });
      }
    }

    await connection.commit();
    res.json(results);
  } catch (error) {
    await connection.rollback();
    console.error("Error importing students:", error);
    res.status(500).json({ error: "Failed to import students" });
  } finally {
    connection.release();
  }
};

// GET /api/students/export - Export students with filter options
const exportStudents = async (req, res) => {
  try {
    const { activeOnly, inactiveOnly } = req.query;

    let whereClause = "";
    let fileName = `students_export_${
      new Date().toISOString().split("T")[0]
    }.xlsx`;

    if (activeOnly === "true") {
      whereClause = "WHERE (s.is_active IS NULL OR s.is_active = TRUE)";
      fileName = `active_students_export_${
        new Date().toISOString().split("T")[0]
      }.xlsx`;
    } else if (inactiveOnly === "true") {
      whereClause = "WHERE s.is_active = FALSE";
      fileName = `inactive_students_export_${
        new Date().toISOString().split("T")[0]
      }.xlsx`;
    }
    // If neither parameter is provided, export all students

    const [students] = await pool.query(`
      SELECT 
        s.admission_number,
        s.first_name,
        s.last_name,
        s.date_of_birth,
        s.gender,
        s.parent_name,
        s.parent_contact,
        s.parent_email,
        s.address,
        s.enrolled_date,
        s.has_fee_block,
        s.is_active,
        c.class_name,
        c.room_number,
        ay.year_label as academic_year,
        s.created_at
      FROM students s
      LEFT JOIN class_assignments ca ON s.id = ca.student_id 
        AND ca.academic_year_id = (SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1)
      LEFT JOIN classes c ON ca.class_id = c.id
      LEFT JOIN academic_years ay ON ca.academic_year_id = ay.id
      ${whereClause}
      ORDER BY c.class_name, s.first_name, s.last_name
    `);

    // Prepare data for Excel
    const excelData = students.map((student) => ({
      "Admission Number": student.admission_number,
      "First Name": student.first_name,
      "Last Name": student.last_name,
      "Date of Birth": student.date_of_birth,
      Gender: student.gender,
      "Parent Name": student.parent_name,
      "Parent Contact": student.parent_contact,
      "Parent Email": student.parent_email,
      Address: student.address,
      "Enrollment Date": student.enrolled_date,
      "Class Name": student.class_name || "Not Assigned",
      "Room Number": student.room_number || "",
      "Academic Year": student.academic_year || "Not Assigned",
      "Fee Block": student.has_fee_block ? "Yes" : "No",
      // "Status": student.is_active === false ? "Inactive" : "Active",
      "Created At": student.created_at,
    }));

    // Create workbook and worksheet
    const workbook = XLSX.utils.book_new();
    const worksheet = XLSX.utils.json_to_sheet(excelData);

    // Add worksheet to workbook
    XLSX.utils.book_append_sheet(workbook, worksheet, "Students");

    // Generate buffer
    const buffer = XLSX.write(workbook, { type: "buffer", bookType: "xlsx" });

    // Set headers for file download
    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    );
    res.setHeader("Content-Disposition", `attachment; filename=${fileName}`);

    res.send(buffer);
  } catch (error) {
    console.error("Error exporting students:", error);
    res.status(500).json({ error: "Failed to export students" });
  }
};

// GET /api/grading-scales - Get all grading scales
const getGradingScales = async (req, res) => {
  try {
    const [scales] = await pool.query(`
      SELECT * FROM grading_scales 
      ORDER BY min_score DESC
    `);
    res.json(scales);
  } catch (error) {
    console.error("Error fetching grading scales:", error);
    res.status(500).json({ error: "Failed to fetch grading scales" });
  }
};

// GET /api/grading-scales/:id - Get specific grading scale
const getGradingScaleById = async (req, res) => {
  try {
    const { id } = req.params;

    const [scales] = await pool.query(
      "SELECT * FROM grading_scales WHERE id = ?",
      [id],
    );

    if (scales.length === 0) {
      return res.status(404).json({ error: "Grading scale not found" });
    }

    res.json(scales[0]);
  } catch (error) {
    console.error("Error fetching grading scale:", error);
    res.status(500).json({ error: "Failed to fetch grading scale" });
  }
};

// POST /api/grading-scales - Create new grading scale
const createGradingScale = async (req, res) => {
  const connection = await pool.getConnection();

  try {
    await connection.beginTransaction();

    const { min_score, max_score, grade, grade_points, remarks } = req.body;

    // Validate score range
    if (min_score < 0 || max_score < 0) {
      await connection.rollback();
      return res.status(400).json({ error: "Scores cannot be negative" });
    }

    if (min_score > max_score) {
      await connection.rollback();
      return res
        .status(400)
        .json({ error: "Minimum score cannot be greater than maximum score" });
    }

    // Check for overlapping ranges
    const [overlapping] = await connection.query(
      `SELECT * FROM grading_scales 
       WHERE (min_score BETWEEN ? AND ?) OR (max_score BETWEEN ? AND ?) 
       OR (? BETWEEN min_score AND max_score) OR (? BETWEEN min_score AND max_score)`,
      [min_score, max_score, min_score, max_score, min_score, max_score],
    );

    if (overlapping.length > 0) {
      await connection.rollback();
      return res.status(400).json({
        error: "Score range overlaps with existing grading scale",
      });
    }

    // Check if grade already exists
    const [existingGrade] = await connection.query(
      "SELECT id FROM grading_scales WHERE grade = ?",
      [grade],
    );

    if (existingGrade.length > 0) {
      await connection.rollback();
      return res.status(400).json({
        error: "Grade letter already exists",
      });
    }

    // Insert grading scale
    const [result] = await connection.query(
      `INSERT INTO grading_scales (min_score, max_score, grade, grade_points, remarks) 
       VALUES (?, ?, ?, ?, ?)`,
      [min_score, max_score, grade, grade_points, remarks],
    );

    await connection.commit();

    // Return the created grading scale
    const [newScale] = await connection.query(
      "SELECT * FROM grading_scales WHERE id = ?",
      [result.insertId],
    );

    res.status(201).json(newScale[0]);
  } catch (error) {
    await connection.rollback();
    console.error("Error creating grading scale:", error);
    res.status(500).json({ error: "Failed to create grading scale" });
  } finally {
    connection.release();
  }
};

// PUT /api/grading-scales/:id - Update grading scale
const updateGradingScale = async (req, res) => {
  const connection = await pool.getConnection();

  try {
    await connection.beginTransaction();

    const { id } = req.params;
    const { min_score, max_score, grade, grade_points, remarks } = req.body;

    // Check if grading scale exists
    const [existingScale] = await connection.query(
      "SELECT id FROM grading_scales WHERE id = ?",
      [id],
    );

    if (existingScale.length === 0) {
      await connection.rollback();
      return res.status(404).json({ error: "Grading scale not found" });
    }

    // Validate score range
    if (min_score < 0 || max_score < 0) {
      await connection.rollback();
      return res.status(400).json({ error: "Scores cannot be negative" });
    }

    if (min_score > max_score) {
      await connection.rollback();
      return res
        .status(400)
        .json({ error: "Minimum score cannot be greater than maximum score" });
    }

    // Check for overlapping ranges (excluding current record)
    const [overlapping] = await connection.query(
      `SELECT * FROM grading_scales 
       WHERE id != ? AND (
         (min_score BETWEEN ? AND ?) OR (max_score BETWEEN ? AND ?) 
         OR (? BETWEEN min_score AND max_score) OR (? BETWEEN min_score AND max_score)
       )`,
      [id, min_score, max_score, min_score, max_score, min_score, max_score],
    );

    if (overlapping.length > 0) {
      await connection.rollback();
      return res.status(400).json({
        error: "Score range overlaps with existing grading scale",
      });
    }

    // Check if grade already exists (excluding current record)
    const [existingGrade] = await connection.query(
      "SELECT id FROM grading_scales WHERE grade = ? AND id != ?",
      [grade, id],
    );

    if (existingGrade.length > 0) {
      await connection.rollback();
      return res.status(400).json({
        error: "Grade letter already exists",
      });
    }

    // Update grading scale
    await connection.query(
      `UPDATE grading_scales SET 
       min_score = ?, max_score = ?, grade = ?, grade_points = ?, remarks = ?
       WHERE id = ?`,
      [min_score, max_score, grade, grade_points, remarks, id],
    );

    await connection.commit();

    // Return the updated grading scale
    const [updatedScale] = await connection.query(
      "SELECT * FROM grading_scales WHERE id = ?",
      [id],
    );

    res.json(updatedScale[0]);
  } catch (error) {
    await connection.rollback();
    console.error("Error updating grading scale:", error);
    res.status(500).json({ error: "Failed to update grading scale" });
  } finally {
    connection.release();
  }
};

// DELETE /api/grading-scales/:id - Delete grading scale
const deleteGradingScale = async (req, res) => {
  try {
    const { id } = req.params;

    const [result] = await pool.query(
      "DELETE FROM grading_scales WHERE id = ?",
      [id],
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ error: "Grading scale not found" });
    }

    res.json({ message: "Grading scale deleted successfully" });
  } catch (error) {
    console.error("Error deleting grading scale:", error);
    res.status(500).json({ error: "Failed to delete grading scale" });
  }
};

// GET /api/grading-scales/calculate/:score - Calculate grade for a score
const calculateGrade = async (req, res) => {
  try {
    const { score } = req.params;

    const [grade] = await pool.query(
      `SELECT * FROM grading_scales 
       WHERE ? BETWEEN min_score AND max_score 
       LIMIT 1`,
      [score],
    );

    if (grade.length === 0) {
      return res.status(404).json({ error: "No grade found for this score" });
    }

    res.json(grade[0]);
  } catch (error) {
    console.error("Error calculating grade:", error);
    res.status(500).json({ error: "Failed to calculate grade" });
  }
};

//manage grades routes
// POST /api/grades/bulk - Create multiple grades at once
const createBulkGrades = async (req, res) => {
  const connection = await pool.getConnection();

  try {
    await connection.beginTransaction();

    const { grades } = req.body;

    if (!grades || !Array.isArray(grades) || grades.length === 0) {
      await connection.rollback();
      return res.status(400).json({ error: "No grades provided" });
    }

    const results = {
      success: 0,
      errors: [],
      updated: 0,
      created: 0,
    };

    for (const gradeData of grades) {
      try {
        const {
          student_id,
          subject_id,
          academic_year_id,
          term_id,
          class_score,
          exam_score,
          maximum_score,
          grade_date,
          notes,
          entered_by,
        } = gradeData;

        // Skip if both scores are 0 or empty
        if (
          !class_score &&
          class_score !== 0 &&
          !exam_score &&
          exam_score !== 0
        ) {
          continue;
        }

        // Check if grade already exists
        const [existingGrade] = await connection.query(
          `SELECT id FROM grades 
           WHERE student_id = ? AND subject_id = ? AND academic_year_id = ? AND term_id = ?`,
          [student_id, subject_id, academic_year_id, term_id],
        );

        const maxScore = maximum_score || 100;
        const finalClassScore = parseFloat(class_score) || 0;
        const finalExamScore = parseFloat(exam_score) || 0;

        // Validate scores
        if (finalClassScore < 0 || finalExamScore < 0) {
          results.errors.push({
            student_id,
            subject_id,
            error: "Scores cannot be negative",
          });
          continue;
        }

        if (finalClassScore > maxScore || finalExamScore > maxScore) {
          results.errors.push({
            student_id,
            subject_id,
            error: "Scores cannot exceed maximum score",
          });
          continue;
        }

        if (existingGrade.length > 0) {
          // Update existing grade
          await connection.query(
            `UPDATE grades SET 
             class_score = ?, exam_score = ?, maximum_score = ?, grade_date = ?, notes = ?
             WHERE id = ?`,
            [
              finalClassScore,
              finalExamScore,
              maxScore,
              grade_date,
              notes,
              existingGrade[0].id,
            ],
          );
          results.updated++;
        } else {
          // Create new grade
          await connection.query(
            `INSERT INTO grades 
             (student_id, subject_id, academic_year_id, term_id, class_score, exam_score, maximum_score, grade_date, notes, entered_by) 
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [
              student_id,
              subject_id,
              academic_year_id,
              term_id,
              finalClassScore,
              finalExamScore,
              maxScore,
              grade_date,
              notes,
              entered_by,
            ],
          );
          results.created++;
        }

        results.success++;
      } catch (error) {
        results.errors.push({
          student_id: gradeData.student_id,
          subject_id: gradeData.subject_id,
          error: error.message,
        });
      }
    }

    if (results.errors.length > 0 && results.success === 0) {
      await connection.rollback();
      return res.status(400).json({
        error: "Failed to create any grades",
        details: results.errors,
      });
    }

    await connection.commit();
    res.status(201).json({
      message: `Successfully processed ${results.success} grade records (${results.created} created, ${results.updated} updated)`,
      success: results.success,
      created: results.created,
      updated: results.updated,
      errors: results.errors,
    });
  } catch (error) {
    await connection.rollback();
    console.error("Error creating bulk grades:", error);
    res.status(500).json({ error: "Failed to create bulk grades" });
  } finally {
    connection.release();
  }
};

//get class subjects
const getClassSubjects = async (req, res) => {
  try {
    const { classId, academicYearId } = req.params;

    const [subjects] = await pool.query(
      `
      SELECT DISTINCT s.* 
      FROM subjects s
      INNER JOIN subject_assignments sa ON s.id = sa.subject_id
      WHERE sa.class_id = ? AND sa.academic_year_id = ?
      ORDER BY s.subject_name
    `,
      [classId, academicYearId],
    );

    res.json(subjects);
  } catch (error) {
    console.error("Error fetching class subjects:", error);
    res.status(500).json({ error: "Failed to fetch class subjects" });
  }
};

// GET /api/grades - Get grades with filters
const getGrades = async (req, res) => {
  try {
    const { class_id, subject_id, term_id, academic_year_id, student_id } =
      req.query;

    let whereConditions = ["1=1"];
    let queryParams = [];

    if (class_id) {
      whereConditions.push(`
        g.student_id IN (
          SELECT student_id FROM class_assignments 
          WHERE class_id = ? AND academic_year_id = COALESCE(?, academic_year_id)
        )
      `);
      queryParams.push(class_id);
      if (academic_year_id) {
        queryParams.push(academic_year_id);
      }
    }

    if (academic_year_id) {
      whereConditions.push("g.academic_year_id = ?");
      queryParams.push(academic_year_id);
    }

    if (subject_id) {
      whereConditions.push("g.subject_id = ?");
      queryParams.push(subject_id);
    }

    if (term_id) {
      whereConditions.push("g.term_id = ?");
      queryParams.push(term_id);
    }

    if (student_id) {
      whereConditions.push("g.student_id = ?");
      queryParams.push(student_id);
    }

    const [grades] = await pool.query(
      `
      SELECT 
        g.*,
        s.first_name,
        s.last_name,
        s.admission_number,
        sub.subject_name,
        sub.subject_code,
        t.term_name,
        ay.year_label as academic_year
      FROM grades g
      INNER JOIN students s ON g.student_id = s.id
      INNER JOIN subjects sub ON g.subject_id = sub.id
      INNER JOIN terms t ON g.term_id = t.id
      INNER JOIN academic_years ay ON g.academic_year_id = ay.id
      WHERE ${whereConditions.join(" AND ")}
      ORDER BY s.first_name, s.last_name
    `,
      queryParams,
    );

    res.json(grades);
  } catch (error) {
    console.error("Error fetching grades:", error);
    res.status(500).json({ error: "Failed to fetch grades" });
  }
};

// import xlsx module
const exportGradeTemplate = async (req, res) => {
  try {
    const { class_id, subject_id, term_id, academic_year_id } = req.query;

    // Get class students
    const [students] = await pool.query(
      `
      SELECT s.id, s.admission_number, s.first_name, s.last_name
      FROM students s
      INNER JOIN class_assignments ca ON s.id = ca.student_id
      WHERE ca.class_id = ? AND ca.academic_year_id = ? AND (s.is_active IS NULL OR s.is_active = TRUE)
      ORDER BY s.first_name, s.last_name
    `,
      [class_id, academic_year_id],
    );

    // Get subject info
    const [subjects] = await pool.query(
      `
      SELECT id, subject_name, subject_code 
      FROM subjects 
      WHERE id = ?
    `,
      [subject_id],
    );

    if (subjects.length === 0) {
      return res.status(404).json({ error: "Subject not found" });
    }

    const subject = subjects[0];

    // Prepare Excel data
    const excelData = students.map((student) => ({
      "Student ID": student.id,
      "Admission Number": student.admission_number,
      "First Name": student.first_name,
      "Last Name": student.last_name,
      "Class Score (0-50)": "",
      "Exam Score (0-50)": "",
      Notes: "",
    }));

    // Create workbook
    const workbook = XLSX.utils.book_new();
    const worksheet = XLSX.utils.json_to_sheet(excelData);

    // Add headers and instructions
    const fileName = `grade_template_${subject.subject_code}_${class_id}_term${term_id}.xlsx`;

    XLSX.utils.book_append_sheet(workbook, worksheet, "Grades");

    // Generate buffer
    const buffer = XLSX.write(workbook, { type: "buffer", bookType: "xlsx" });

    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    );
    res.setHeader("Content-Disposition", `attachment; filename=${fileName}`);

    res.send(buffer);
  } catch (error) {
    console.error("Error exporting grade template:", error);
    res.status(500).json({ error: "Failed to export grade template" });
  }
};

// Add to control.js
const importGrades = async (req, res) => {
  const connection = await pool.getConnection();

  try {
    await connection.beginTransaction();

    if (!req.file) {
      await connection.rollback();
      return res.status(400).json({ error: "No file uploaded" });
    }

    // Read Excel file
    const workbook = XLSX.read(req.file.buffer, { type: "buffer" });
    const worksheet = workbook.Sheets[workbook.SheetNames[0]];
    const gradeData = XLSX.utils.sheet_to_json(worksheet);

    const results = {
      success: 0,
      errors: [],
      created: 0,
      updated: 0,
    };

    const { subject_id, term_id, academic_year_id, entered_by } = req.body;

    // Get default user if none provided
    const [defaultUser] = await connection.query(
      "SELECT id FROM users LIMIT 1",
    );
    const defaultUserId = defaultUser.length > 0 ? defaultUser[0].id : null;

    for (const row of gradeData) {
      try {
        const studentId = row["Student ID"];
        const classScore = parseFloat(row["Class Score (0-50)"]);
        const examScore = parseFloat(row["Exam Score (0-50)"]);
        const notes = row["Notes"] || "Imported via Excel";

        // Skip if no scores provided
        if (isNaN(classScore) && isNaN(examScore)) {
          continue;
        }

        // Validate scores
        if (
          (!isNaN(classScore) && (classScore < 0 || classScore > 50)) ||
          (!isNaN(examScore) && (examScore < 0 || examScore > 50))
        ) {
          results.errors.push({
            student_id: studentId,
            admission_number: row["Admission Number"],
            error: "Scores must be between 0 and 50",
          });
          continue;
        }

        const finalClassScore = isNaN(classScore) ? 0 : classScore;
        const finalExamScore = isNaN(examScore) ? 0 : examScore;

        // Check if grade exists
        const [existingGrade] = await connection.query(
          `SELECT id FROM grades 
           WHERE student_id = ? AND subject_id = ? AND academic_year_id = ? AND term_id = ?`,
          [studentId, subject_id, academic_year_id, term_id],
        );

        if (existingGrade.length > 0) {
          // Update existing
          await connection.query(
            `UPDATE grades SET 
             class_score = ?, exam_score = ?, notes = ?, entered_by = ?
             WHERE id = ?`,
            [
              finalClassScore,
              finalExamScore,
              notes,
              entered_by || defaultUserId,
              existingGrade[0].id,
            ],
          );
          results.updated++;
        } else {
          // Create new
          await connection.query(
            `INSERT INTO grades 
             (student_id, subject_id, academic_year_id, term_id, class_score, exam_score, maximum_score, grade_date, notes, entered_by) 
             VALUES (?, ?, ?, ?, ?, ?, 100, CURDATE(), ?, ?)`,
            [
              studentId,
              subject_id,
              academic_year_id,
              term_id,
              finalClassScore,
              finalExamScore,
              notes,
              entered_by || defaultUserId,
            ],
          );
          results.created++;
        }

        results.success++;
      } catch (error) {
        results.errors.push({
          student_id: row["Student ID"],
          admission_number: row["Admission Number"],
          error: error.message,
        });
      }
    }

    await connection.commit();

    res.json({
      message: `Imported ${results.success} grade records (${results.created} new, ${results.updated} updated)`,
      ...results,
    });
  } catch (error) {
    await connection.rollback();
    console.error("Error importing grades:", error);
    res.status(500).json({ error: "Failed to import grades" });
  } finally {
    connection.release();
  }
};

// GET /api/report-cards - Get report cards with filters AND PAGINATION
const getReportCards = async (req, res) => {
  try {
    const {
      student_id,
      academic_year_id,
      term_id,
      class_id,
      page = 1,
      limit = 20,
      search = "",
      sort_by = "s.first_name",
      sort_order = "asc",
      status = "all", // all, excellent, good, needs_improvement
    } = req.query;

    const pageNum = parseInt(page);
    const limitNum = parseInt(limit);
    const offset = (pageNum - 1) * limitNum;

    // Build WHERE conditions
    let whereConditions = ["1=1"];
    let queryParams = [];

    if (student_id) {
      whereConditions.push("rc.student_id = ?");
      queryParams.push(student_id);
    }

    if (academic_year_id) {
      whereConditions.push("rc.academic_year_id = ?");
      queryParams.push(academic_year_id);
    }

    if (term_id) {
      whereConditions.push("rc.term_id = ?");
      queryParams.push(term_id);
    }

    if (class_id) {
      whereConditions.push(`
        rc.student_id IN (
          SELECT student_id FROM class_assignments 
          WHERE class_id = ? AND academic_year_id = rc.academic_year_id
        )
      `);
      queryParams.push(class_id);
    }

    // Add search filter
    if (search) {
      whereConditions.push(
        "(s.first_name LIKE ? OR s.last_name LIKE ? OR s.admission_number LIKE ? OR c.class_name LIKE ?)",
      );
      queryParams.push(
        `%${search}%`,
        `%${search}%`,
        `%${search}%`,
        `%${search}%`,
      );
    }

    // Add status filter
    if (status !== "all") {
      if (status === "excellent") {
        whereConditions.push("rc.overall_total >= 80");
      } else if (status === "good") {
        whereConditions.push(
          "rc.overall_total >= 60 AND rc.overall_total < 80",
        );
      } else if (status === "needs_improvement") {
        whereConditions.push("rc.overall_total < 60");
      }
    }

    // Get total count first
    const [countResult] = await pool.query(
      `SELECT COUNT(DISTINCT rc.id) as total
       FROM report_cards rc
       INNER JOIN students s ON rc.student_id = s.id
       LEFT JOIN class_assignments ca ON rc.student_id = ca.student_id AND rc.academic_year_id = ca.academic_year_id
       LEFT JOIN classes c ON ca.class_id = c.id
       INNER JOIN academic_years ay ON rc.academic_year_id = ay.id
       INNER JOIN terms t ON rc.term_id = t.id
       LEFT JOIN users u ON rc.issued_by = u.id
       WHERE ${whereConditions.join(" AND ")}`,
      queryParams,
    );

    const total = countResult[0].total;
    const totalPages = Math.ceil(total / limitNum);

    // Validate sort parameters
    const validSortFields = {
      student_name: "s.first_name, s.last_name",
      class: "c.class_name",
      score: "rc.overall_total",
      position: "rc.overall_position",
      date: "rc.date_issued",
      admission_number: "s.admission_number",
    };

    const validSortOrders = ["asc", "desc"];

    const sortField = validSortFields[sort_by] || "s.first_name, s.last_name";
    const sortOrder = validSortOrders.includes(sort_order.toLowerCase())
      ? sort_order.toUpperCase()
      : "ASC";

    // Get paginated data
    const [reportCards] = await pool.query(
      `
      SELECT 
        rc.*,
        s.first_name,
        s.last_name,
        s.admission_number,
        c.class_name,
        ay.year_label as academic_year,
        t.term_name,
        u.username as issued_by_name
      FROM report_cards rc
      INNER JOIN students s ON rc.student_id = s.id
      LEFT JOIN class_assignments ca ON rc.student_id = ca.student_id AND rc.academic_year_id = ca.academic_year_id
      LEFT JOIN classes c ON ca.class_id = c.id
      INNER JOIN academic_years ay ON rc.academic_year_id = ay.id
      INNER JOIN terms t ON rc.term_id = t.id
      LEFT JOIN users u ON rc.issued_by = u.id
      WHERE ${whereConditions.join(" AND ")}
      GROUP BY rc.id
      ORDER BY ${sortField} ${sortOrder}
      LIMIT ? OFFSET ?
      `,
      [...queryParams, limitNum, offset],
    );

    res.json({
      reportCards,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total,
        totalPages,
        hasNextPage: pageNum < totalPages,
        hasPrevPage: pageNum > 1,
      },
      filters: {
        academic_year_id,
        term_id,
        class_id,
        search,
        status,
        sort_by,
        sort_order,
      },
    });
  } catch (error) {
    console.error("Error fetching report cards:", error);
    res.status(500).json({ error: "Failed to fetch report cards" });
  }
};

// GET /api/report-cards/:id/pdf - Generate PDF for individual report card
const getIndividualReportCardPDF = async (req, res) => {
  try {
    const { report_card_id } = req.params;

    // Call the existing function to generate PDF
    const pdfBuffer = await generateStudentReportCardPDF({
      params: { report_card_id },
    });

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="report-card-${report_card_id}.pdf"`,
    );
    res.send(pdfBuffer);
  } catch (error) {
    console.error("Error generating individual report card PDF:", error);
    res.status(500).json({ error: "Failed to generate PDF" });
  }
};

// GET /api/report-cards/:id - Get specific report card with details
const getReportCardById = async (req, res) => {
  try {
    const { id } = req.params;

    const [reportCards] = await pool.query(
      `
      SELECT 
        rc.*,
        s.first_name,
        s.last_name,
        s.admission_number,
        s.date_of_birth,
        s.gender,
        c.class_name as current_class_name,
        COALESCE(rc.promoted_class_name, c_promoted.class_name) as promoted_to_class_name,
        ay.year_label as academic_year,
        t.term_name,
        u.username as issued_by_name
      FROM report_cards rc
      INNER JOIN students s ON rc.student_id = s.id
      LEFT JOIN class_assignments ca ON rc.student_id = ca.student_id AND rc.academic_year_id = ca.academic_year_id
      LEFT JOIN classes c ON ca.class_id = c.id
      LEFT JOIN classes c_promoted ON rc.promoted_to_class_id = c_promoted.id
      INNER JOIN academic_years ay ON rc.academic_year_id = ay.id
      INNER JOIN terms t ON rc.term_id = t.id
      LEFT JOIN users u ON rc.issued_by = u.id
      WHERE rc.id = ?
    `,
      [id],
    );

    if (reportCards.length === 0) {
      return res.status(404).json({ error: "Report card not found" });
    }

    const reportCard = reportCards[0];

    // Get report card details (subject grades)
    const [details] = await pool.query(
      `
      SELECT 
        rcd.*,
        s.subject_name,
        s.subject_code
      FROM report_card_details rcd
      INNER JOIN subjects s ON rcd.subject_id = s.id
      WHERE rcd.report_card_id = ?
      ORDER BY s.subject_name
    `,
      [id],
    );

    // Calculate statistics
    const totalSubjects = details.length;
    const averageScore =
      details.length > 0
        ? details.reduce(
            (sum, detail) => sum + parseFloat(detail.subject_total || 0),
            0,
          ) / details.length
        : 0;

    res.json({
      ...reportCard,
      details,
      statistics: {
        totalSubjects,
        averageScore: averageScore.toFixed(2),
      },
    });
  } catch (error) {
    console.error("Error fetching report card:", error);
    res.status(500).json({ error: "Failed to fetch report card" });
  }
};

//generateReportCards function
const generateReportCards = async (req, res) => {
  const connection = await pool.getConnection();

  try {
    await connection.beginTransaction();

    const { class_id, academic_year_id, term_id, issued_by } = req.body;

    // Get all students in the class
    const [students] = await pool.query(
      `
      SELECT s.id as student_id, s.first_name, s.last_name, s.admission_number
      FROM students s
      INNER JOIN class_assignments ca ON s.id = ca.student_id
      WHERE ca.class_id = ? AND ca.academic_year_id = ? AND (s.is_active IS NULL OR s.is_active = TRUE)
      ORDER BY s.first_name, s.last_name
    `,
      [class_id, academic_year_id],
    );

    if (students.length === 0) {
      await connection.rollback();
      return res.status(400).json({ error: "No students found in this class" });
    }

    const results = {
      generated: 0,
      errors: [],
      skipped: 0,
      created: 0,
      updated: 0,
    };

    // Calculate subject positions AND overall positions
    const subjectPositions = await calculateSubjectPositions(
      class_id,
      academic_year_id,
      term_id,
    );
    const overallPositions = await calculateOverallPositions(
      class_id,
      academic_year_id,
      term_id,
    );

    for (const student of students) {
      try {
        // Check if report card already exists
        const [existing] = await connection.query(
          "SELECT id FROM report_cards WHERE student_id = ? AND academic_year_id = ? AND term_id = ?",
          [student.student_id, academic_year_id, term_id],
        );

        if (existing.length > 0) {
          results.skipped++;
          results.errors.push({
            student: `${student.first_name} ${student.last_name}`,
            error: "Report card already exists",
          });
          continue;
        }

        // Get student's grades for this term
        const [grades] = await connection.query(
          `
          SELECT 
            g.subject_id,
            g.class_score,
            g.exam_score,
            g.subject_total,
            s.subject_name,
            s.subject_code
          FROM grades g
          INNER JOIN subjects s ON g.subject_id = s.id
          WHERE g.student_id = ? AND g.academic_year_id = ? AND g.term_id = ?
          ORDER BY s.subject_name
        `,
          [student.student_id, academic_year_id, term_id],
        );

        if (grades.length === 0) {
          results.errors.push({
            student: `${student.first_name} ${student.last_name}`,
            error: "No grades found for this term",
          });
          continue;
        }

        // Calculate overall total from all subjects
        const overallTotal = grades.reduce(
          (sum, grade) => sum + parseFloat(grade.subject_total || 0),
          0,
        );

        // Get overall position for this student
        const overallPosition = overallPositions[student.student_id] || null;

        // Get attendance - FIXED: Remove undefined date parameters
        const attendanceData = await getStudentAttendance(
          student.student_id,
          academic_year_id,
          term_id,
          // Removed termStartDate and termEndDate parameters
        );

        // Create report card WITH overall position
        const [reportCardResult] = await connection.query(
          `INSERT INTO report_cards 
           (student_id, academic_year_id, term_id, overall_total, overall_position, 
            attendance_days, total_days, date_issued, issued_by) 
           VALUES (?, ?, ?, ?, ?, ?, ?, CURDATE(), ?)`,
          [
            student.student_id,
            academic_year_id,
            term_id,
            overallTotal,
            overallPosition,
            attendanceData.present_days,
            attendanceData.total_days,
            issued_by,
          ],
        );

        const reportCardId = reportCardResult.insertId;

        // Create report card details
        for (const grade of grades) {
          const [gradeInfo] = await connection.query(
            `SELECT grade, remarks FROM grading_scales 
             WHERE ? BETWEEN min_score AND max_score LIMIT 1`,
            [grade.subject_total],
          );

          // Get subject position for this student
          const subjectPosition =
            subjectPositions[grade.subject_id]?.[student.student_id] || null;

          await connection.query(
            `INSERT INTO report_card_details 
             (report_card_id, subject_id, class_score, exam_score, subject_total, subject_position, grade, remarks) 
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
            [
              reportCardId,
              grade.subject_id,
              grade.class_score,
              grade.exam_score,
              grade.subject_total,
              subjectPosition,
              gradeInfo.length > 0 ? gradeInfo[0].grade : "N/A",
              gradeInfo.length > 0 ? gradeInfo[0].remarks : "No grade",
            ],
          );
        }

        results.generated++;
        results.created++;
      } catch (error) {
        results.errors.push({
          student: `${student.first_name} ${student.last_name}`,
          error: error.message,
        });
      }
    }

    await connection.commit();

    res.status(201).json({
      message: `Generated ${results.generated} report cards, skipped ${results.skipped} existing`,
      ...results,
    });
  } catch (error) {
    await connection.rollback();
    console.error("Error generating report cards:", error);
    res.status(500).json({
      error: "Failed to generate report cards",
      details: error.message,
    });
  } finally {
    connection.release();
  }
};

const calculateSubjectPositions = async (
  class_id,
  academic_year_id,
  term_id,
) => {
  try {
    // Get all subjects taught in this class
    const [subjects] = await pool.query(
      `
      SELECT DISTINCT s.id as subject_id, s.subject_name
      FROM subjects s
      INNER JOIN subject_assignments sa ON s.id = sa.subject_id
      WHERE sa.class_id = ? AND sa.academic_year_id = ?
    `,
      [class_id, academic_year_id],
    );

    const subjectPositions = {};

    for (const subject of subjects) {
      // Get all students' grades for this subject, ordered by score (descending)
      const [grades] = await pool.query(
        `
        SELECT 
          g.student_id,
          g.subject_total,
          s.first_name,
          s.last_name
        FROM grades g
        INNER JOIN students s ON g.student_id = s.id
        WHERE g.subject_id = ? 
          AND g.academic_year_id = ? 
          AND g.term_id = ?
          AND g.subject_total IS NOT NULL
        ORDER BY g.subject_total DESC
      `,
        [subject.subject_id, academic_year_id, term_id],
      );

      // Calculate positions (handle ties)
      let currentPosition = 0;
      let previousScore = null;
      let skipCount = 0;

      subjectPositions[subject.subject_id] = {};

      for (let i = 0; i < grades.length; i++) {
        const grade = grades[i];

        if (grade.subject_total !== previousScore) {
          currentPosition = currentPosition + 1 + skipCount;
          skipCount = 0;
        } else {
          // Same score as previous student - same position
          skipCount++;
        }

        subjectPositions[subject.subject_id][grade.student_id] =
          currentPosition;
        previousScore = grade.subject_total;
      }
    }

    return subjectPositions;
  } catch (error) {
    console.error("Error calculating subject positions:", error);
    return {};
  }
};

// Calculate overall positions based on sum of all subject totals
const calculateOverallPositions = async (
  class_id,
  academic_year_id,
  term_id,
) => {
  try {
    // Get all students with their total scores across all subjects
    const [studentTotals] = await pool.query(
      `
      SELECT 
        g.student_id,
        s.first_name,
        s.last_name,
        s.admission_number,
        SUM(g.subject_total) as total_score,
        COUNT(g.subject_id) as subject_count
      FROM grades g
      INNER JOIN students s ON g.student_id = s.id
      INNER JOIN class_assignments ca ON s.id = ca.student_id AND ca.academic_year_id = g.academic_year_id
      WHERE ca.class_id = ? 
        AND g.academic_year_id = ? 
        AND g.term_id = ?
        AND g.subject_total IS NOT NULL
      GROUP BY g.student_id, s.first_name, s.last_name, s.admission_number
      HAVING COUNT(g.subject_id) > 0
      ORDER BY total_score DESC
    `,
      [class_id, academic_year_id, term_id],
    );

    const overallPositions = {};
    let currentPosition = 0;
    let previousTotal = null;
    let skipCount = 0;

    for (let i = 0; i < studentTotals.length; i++) {
      const student = studentTotals[i];

      if (student.total_score !== previousTotal) {
        currentPosition = currentPosition + 1 + skipCount;
        skipCount = 0;
      } else {
        // Same total score as previous student - same position
        skipCount++;
      }

      overallPositions[student.student_id] = currentPosition;
      previousTotal = student.total_score;
    }

    return overallPositions;
  } catch (error) {
    console.error("Error calculating overall positions:", error);
    return {};
  }
};

// PUT /api/report-cards/:id - Update report card comments
const updateReportCard = async (req, res) => {
  try {
    const { id } = req.params;
    const {
      overall_comment,
      teacher_comment,
      principal_comment,
      overall_position,
      attendance_days,
      total_days,
      student_interest,
      promoted_class_name, // New: "Basic 2A", "Basic 1A", etc.
      promoted_to_class_id, // Keep for reference if needed
    } = req.body;

    await pool.query(
      `UPDATE report_cards SET 
       overall_comment = ?, teacher_comment = ?, principal_comment = ?, 
       overall_position = ?, attendance_days = ?, total_days = ?,
       student_interest = ?, promoted_class_name = ?, promoted_to_class_id = ?
       WHERE id = ?`,
      [
        overall_comment,
        teacher_comment,
        principal_comment,
        overall_position,
        attendance_days,
        total_days,
        student_interest,
        promoted_class_name || null, // Store the class name
        promoted_to_class_id || null,
        id,
      ],
    );

    const [updatedReportCard] = await pool.query(
      `SELECT rc.*, 
              c_promoted.class_name as promoted_to_class_name
       FROM report_cards rc
       LEFT JOIN classes c_promoted ON rc.promoted_to_class_id = c_promoted.id
       WHERE rc.id = ?`,
      [id],
    );

    res.json(updatedReportCard[0]);
  } catch (error) {
    console.error("Error updating report card:", error);
    res.status(500).json({ error: "Failed to update report card" });
  }
};

// New function for bulk PDF generation
const generateClassReportCardsPDF = async (req, res) => {
  try {
    const { class_id, academic_year_id, term_id } = req.params;

    // Get all students in the class with their report cards
    const [students] = await pool.query(
      `
      SELECT 
        s.id as student_id,
        s.first_name,
        s.last_name, 
        s.admission_number,
        rc.id as report_card_id
      FROM students s
      INNER JOIN class_assignments ca ON s.id = ca.student_id
      LEFT JOIN report_cards rc ON s.id = rc.student_id 
        AND rc.academic_year_id = ? 
        AND rc.term_id = ?
      WHERE ca.class_id = ? AND ca.academic_year_id = ?
      ORDER BY s.first_name, s.last_name
      `,
      [academic_year_id, term_id, class_id, academic_year_id],
    );

    if (students.length === 0) {
      return res.status(404).json({ error: "No students found in this class" });
    }

    // Get class info for filename
    const [classInfo] = await pool.query(
      "SELECT class_name FROM classes WHERE id = ?",
      [class_id],
    );

    const className = classInfo[0]?.class_name || "class";

    // Create a new PDF document for combining
    const PDFDocument = require("pdf-lib").PDFDocument;
    const mergedPdf = await PDFDocument.create();

    // Generate PDF for each student and merge
    const studentsWithReportCards = students.filter(
      (student) => student.report_card_id,
    );

    if (studentsWithReportCards.length === 0) {
      return res.status(400).json({
        error: "No report cards found for students in this class/term",
      });
    }

    for (const student of studentsWithReportCards) {
      try {
        // Use your existing function to generate individual PDF
        const pdfBuffer = await generateStudentReportCardPDF({
          params: { report_card_id: student.report_card_id },
        });

        // Load the individual PDF
        const individualPdf = await PDFDocument.load(pdfBuffer);

        // Copy all pages to the merged PDF
        const pages = await mergedPdf.copyPages(
          individualPdf,
          individualPdf.getPageIndices(),
        );
        pages.forEach((page) => mergedPdf.addPage(page));
      } catch (error) {
        console.error(
          `Error generating PDF for student ${student.first_name} ${student.last_name}:`,
          error,
        );
        // Continue with other students even if one fails
      }
    }

    // Save the merged PDF
    const mergedPdfBuffer = Buffer.from(await mergedPdf.save());

    // Send the combined PDF
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${className}-all-report-cards-${academic_year_id}-${term_id}.pdf"`,
    );

    res.send(mergedPdfBuffer);
  } catch (error) {
    console.error("Error generating class report cards PDF:", error);
    res
      .status(500)
      .json({ error: "Failed to generate class report cards PDF" });
  }
};

// Helper function to get local image as base64
const getLocalImageBase64 = (photoFilename) => {
  try {
    if (!photoFilename) return null;

    const photoPath = path.join(
      __dirname,
      "../uploads/students",
      photoFilename,
    );

    if (fs.existsSync(photoPath)) {
      const imageBuffer = fs.readFileSync(photoPath);
      const base64 = imageBuffer.toString("base64");

      // Determine MIME type from file extension
      const ext = path.extname(photoFilename).toLowerCase();
      const mimeTypes = {
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".gif": "image/gif",
        ".webp": "image/webp",
      };

      const mimeType = mimeTypes[ext] || "image/jpeg";
      return `data:${mimeType};base64,${base64}`;
    } else {
      return null;
    }
  } catch (error) {
    console.error("Error reading local image:", error.message);
    return null;
  }
};

// Helper function for photo placeholder
const addPhotoPlaceholder = (doc, x, y, width, height, student) => {
  // Placeholder background
  doc.setFillColor(240, 240, 240);
  doc.rect(x, y, width, height, "F");

  // Border
  doc.setDrawColor(200, 200, 200);
  doc.setLineWidth(0.5);
  doc.rect(x, y, width, height);

  // Student initials
  doc.setTextColor(150, 150, 150);
  doc.setFontSize(12);
  const initials =
    `${student.first_name[0]}${student.last_name[0]}`.toUpperCase();
  doc.text(initials, x + width / 2, y + height / 2 - 3, { align: "center" });

  // "No Photo" text
  doc.setFontSize(6);
  doc.text("Photo Not", x + width / 2, y + height / 2 + 3, { align: "center" });
  doc.text("Available", x + width / 2, y + height / 2 + 6, { align: "center" });
};

//FIXED getStudentAttendance function
const getStudentAttendance = async (
  studentId,
  academicYearId,
  termId,
  // Remove the unused date parameters
) => {
  try {
    const [attendanceRecords] = await pool.query(
      `
      SELECT 
        COUNT(*) as total_days,
        SUM(CASE WHEN status = 'Present' THEN 1 ELSE 0 END) as present_days,
        SUM(CASE WHEN status = 'Absent' THEN 1 ELSE 0 END) as absent_days,
        SUM(CASE WHEN status = 'Late' THEN 1 ELSE 0 END) as late_days,
        SUM(CASE WHEN status = 'Excused' THEN 1 ELSE 0 END) as excused_days
      FROM attendance 
      WHERE student_id = ? 
        AND academic_year_id = ? 
        AND term_id = ?
    `,
      [studentId, academicYearId, termId],
    );

    const attendanceData = attendanceRecords[0];

    // If we have attendance records, use them
    if (attendanceData.total_days > 0) {
      return {
        source: "attendance_table",
        present_days: attendanceData.present_days || 0,
        total_days: attendanceData.total_days || 0,
        attendance_percentage:
          attendanceData.total_days > 0
            ? Math.round(
                (attendanceData.present_days / attendanceData.total_days) * 100,
              )
            : 0,
      };
    }

    // Default: No attendance data found
    return {
      source: "none",
      present_days: 0,
      total_days: 0,
      attendance_percentage: 0,
    };
  } catch (error) {
    console.error("Error fetching attendance:", error);
    // Return default values on error
    return {
      source: "error",
      present_days: 0,
      total_days: 0,
      attendance_percentage: 0,
    };
  }
};

const generateStudentReportCardPDF = async (req, res = null) => {
  try {
    const { report_card_id } = req.params || req;

    // Get report card data
    const [reportCards] = await pool.query(
      `
      SELECT 
        rc.*,
        s.first_name,
        s.last_name,
        s.admission_number,
        s.date_of_birth,
        s.gender,
        s.parent_name,
        s.parent_contact,
        s.photo_filename,
        c.class_name,
        c_promoted.class_name as promoted_to_class_name,
        ay.year_label as academic_year,
        t.term_name,
        u.username as issued_by_name
      FROM report_cards rc
      INNER JOIN students s ON rc.student_id = s.id
      LEFT JOIN class_assignments ca ON rc.student_id = ca.student_id AND rc.academic_year_id = ca.academic_year_id
      LEFT JOIN classes c ON ca.class_id = c.id
      LEFT JOIN classes c_promoted ON rc.promoted_to_class_id = c_promoted.id
      INNER JOIN academic_years ay ON rc.academic_year_id = ay.id
      INNER JOIN terms t ON rc.term_id = t.id
      LEFT JOIN users u ON rc.issued_by = u.id
      WHERE rc.id = ?
    `,
      [report_card_id],
    );

    if (reportCards.length === 0) {
      if (res) {
        return res.status(404).json({ error: "Report card not found" });
      }
      throw new Error("Report card not found");
    }

    const reportCard = reportCards[0];

    // Get report card details
    const [details] = await pool.query(
      `
      SELECT 
        rcd.*,
        s.subject_name,
        s.subject_code
      FROM report_card_details rcd
      INNER JOIN subjects s ON rcd.subject_id = s.id
      WHERE rcd.report_card_id = ?
      ORDER BY s.subject_name
    `,
      [report_card_id],
    );

    // Get fresh attendance data (in case it changed after report card generation)
    const freshAttendanceData = await getStudentAttendance(
      reportCard.student_id,
      reportCard.academic_year_id,
      reportCard.term_id,
      // termStartDate,
      // termEndDate
    );

    // Use the fresh data, but fall back to report card data if needed
    const attendanceData = {
      present_days:
        freshAttendanceData.present_days || reportCard.attendance_days || 0,
      total_days: freshAttendanceData.total_days || reportCard.total_days || 0,
      source: freshAttendanceData.source,
    };

    // Import jsPDF and autoTable
    const { jsPDF } = require("jspdf");
    const { autoTable } = require("jspdf-autotable");

    // Create PDF
    const doc = new jsPDF({
      orientation: "portrait",
      unit: "mm",
      format: "a4",
    });

    const schoolSettings = await getSchoolSettingsForPDF();

    // Set response headers if this is a route handler
    if (res) {
      res.setHeader("Content-Type", "application/pdf");
      res.setHeader(
        "Content-Disposition",
        `attachment; filename="report-card-${reportCard.admission_number}-${reportCard.term_name}.pdf"`,
      );
    }

    // Colors
    const primaryColor = [41, 128, 185];
    const accentColor = [173, 216, 230];

    // Helper function for ordinal suffixes
    const getOrdinalSuffix = (position) => {
      if (!position) return "N/A";
      if (position === 1) return "1st";
      if (position === 2) return "2nd";
      if (position === 3) return "3rd";
      return `${position}th`;
    };

    // Get student photo as base64 from local storage
    let studentPhotoBase64 = null;
    if (reportCard.photo_filename) {
      studentPhotoBase64 = getLocalImageBase64(reportCard.photo_filename);
    }

    const hasLogo = await addSchoolLogoToPDF(doc, 15, 8, 15, 15);
    const schoolNameX = hasLogo ? 32 : 20;

    // Determine page width from jsPDF (supports different jsPDF versions) with fallback to A4 width (210mm)
    const pageWidth =
      (doc &&
        doc.internal &&
        doc.internal.pageSize &&
        (doc.internal.pageSize.width ||
          (doc.internal.pageSize.getWidth &&
            doc.internal.pageSize.getWidth()))) ||
      210;

    // School name
    doc.setFontSize(12);
    doc.setFont("helvetica", "bold");
    doc.setTextColor(...primaryColor);
    doc.text(schoolSettings.school_name, schoolNameX, 18);

    // School motto
    if (schoolSettings.motto) {
      doc.setFontSize(8);
      doc.setFont("helvetica", "italic");
      doc.setTextColor(100, 100, 100);
      doc.text(schoolSettings.motto, schoolNameX, 23);
    }

    // Contact info on right side
    doc.setFontSize(8);
    doc.setFont("helvetica", "normal");
    doc.setTextColor(0, 0, 0);

    const rightStartX = pageWidth - 75;
    let rightY = 16;

    // Phone
    if (
      schoolSettings.phone_numbers &&
      schoolSettings.phone_numbers.length > 0
    ) {
      doc.text(
        `Phone: ${schoolSettings.phone_numbers[0]}`,
        rightStartX,
        rightY,
      );
      rightY += 3;
    }

    // Email
    if (schoolSettings.email) {
      doc.text(`Email: ${schoolSettings.email}`, rightStartX, rightY);
      rightY += 3;
    }

    // Website
    if (schoolSettings.website) {
      doc.text(`Web: ${schoolSettings.website}`, rightStartX, rightY);
    }

    // Top divider line
    doc.setDrawColor(200, 200, 200);
    doc.setLineWidth(0.3);
    doc.line(15, 28, pageWidth - 15, 28);

    // Student Photo - Top Right Corner (Passport Size: 35x45mm)
    const photoX = doc.internal.pageSize.width - 40;
    const photoY = 30;
    const photoWidth = 30;
    const photoHeight = 40;

    if (studentPhotoBase64) {
      try {
        // Add the actual student photo
        doc.addImage({
          imageData: studentPhotoBase64,
          x: photoX,
          y: photoY,
          width: photoWidth,
          height: photoHeight,
        });

        // Add border around photo
        doc.setDrawColor(100, 100, 100);
        doc.setLineWidth(0.3);
        doc.rect(photoX, photoY, photoWidth, photoHeight);
      } catch (photoError) {
        console.error("Error adding student photo to PDF:", photoError);
        // Fallback to placeholder
        addPhotoPlaceholder(
          doc,
          photoX,
          photoY,
          photoWidth,
          photoHeight,
          reportCard,
        );
      }
    } else {
      // No photo available - show placeholder
      addPhotoPlaceholder(
        doc,
        photoX,
        photoY,
        photoWidth,
        photoHeight,
        reportCard,
      );
    }

    // Report title (adjusted for photo space)
    doc.setFontSize(12);
    doc.setFont("helvetica", "bold");
    doc.setTextColor(...primaryColor);
    doc.text("STUDENT REPORT CARD", doc.internal.pageSize.width / 2 - 2, 32, {
      align: "center",
    });

    // Divider line (shorter to avoid photo area)
    // doc.setDrawColor(...accentColor);
    // doc.setLineWidth(0.5);
    // doc.line(15, 35, doc.internal.pageSize.width - 60, 35);

    let startY = 45;

    // 2. STUDENT INFORMATION (Adjusted to account for photo space)
    doc.setFontSize(12);
    doc.setFont("helvetica", "bold");
    doc.setTextColor(0, 0, 0);
    doc.text("STUDENT INFORMATION", 15, startY);

    startY += 8;

    doc.setFontSize(10);
    doc.setFont("helvetica", "normal");

    // Student details in two columns (adjusted width to avoid photo)
    const studentInfo = [
      [
        `Name: ${reportCard.first_name} ${reportCard.last_name}`,
        `Admission No: ${reportCard.admission_number}`,
      ],
      [
        `Class: ${reportCard.class_name}`,
        `Gender: ${reportCard.gender || "Not specified"}`,
      ],
      [
        `Academic Year: ${reportCard.academic_year}`,
        `Term: ${reportCard.term_name}`,
      ],
      [
        `Date of Birth: ${
          reportCard.date_of_birth
            ? new Date(reportCard.date_of_birth).toLocaleDateString()
            : "N/A"
        }`,
        `Parent: ${reportCard.parent_name || "N/A"}`,
      ],
    ];

    studentInfo.forEach(([left, right], index) => {
      doc.text(left, 15, startY + index * 5);
      doc.text(right, 100, startY + index * 5);
    });

    startY += 25;

    // 3. PERFORMANCE SUMMARY
    doc.setFontSize(11);
    doc.setFont("helvetica", "bold");
    doc.text(
      `Overall Total Score: ${reportCard.overall_total || 0}`,
      15,
      startY,
    );
    doc.text(
      `Class Position: ${getOrdinalSuffix(reportCard.overall_position)}`,
      100,
      startY,
    );

    startY += 5;

    startY += 15;

    // 4. SUBJECTS TABLE USING AUTOTABLE
    const tableData = details.map((detail) => [
      detail.subject_name,
      detail.class_score || 0,
      detail.exam_score || 0,
      detail.subject_total || 0,
      getOrdinalSuffix(detail.subject_position),
      detail.grade || "N/A",
      detail.remarks || "No remarks",
    ]);

    autoTable(doc, {
      startY: 90, // Increased startY to account for photo space
      head: [
        [
          "Subject",
          "Class Score",
          "Exam Score",
          "Total",
          "Position",
          "Grade",
          "Remarks",
        ],
      ],
      body: tableData,
      headStyles: {
        fillColor: primaryColor,
        textColor: [255, 255, 255],
        fontStyle: "bold",
      },
      alternateRowStyles: {
        fillColor: [240, 240, 240],
      },
      margin: { left: 15, right: 15 },
      styles: {
        fontSize: 9,
        cellPadding: 3,
        overflow: "linebreak",
        halign: "center",
      },
      columnStyles: {
        0: { halign: "left", cellWidth: "auto" },
        5: { halign: "left", cellWidth: "auto" },
      },
    });

    // 5. ADDITIONAL INFORMATION SECTION
    const tableEndY = doc.lastAutoTable.finalY + 10;

    // Only show this section if any of the new fields have data
    const hasAdditionalInfo =
      reportCard.attendance_days ||
      reportCard.total_days ||
      reportCard.student_interest ||
      reportCard.promoted_to_class_name;

    if (hasAdditionalInfo) {
      doc.setFontSize(12);
      doc.setFont("helvetica", "bold");
      doc.setTextColor(...primaryColor);
      doc.text("ADDITIONAL INFORMATION", 15, tableEndY);

      let infoY = tableEndY + 8;
      doc.setFontSize(10);
      doc.setFont("helvetica", "normal");
      doc.setTextColor(0, 0, 0);

      // Attendance Information
      if (reportCard.attendance_days || reportCard.total_days) {
        const attendancePercentage =
          attendanceData.total_days > 0
            ? Math.round(
                (attendanceData.present_days / attendanceData.total_days) * 100,
              )
            : 0;

        doc.text(
          `Attendance Record: ${attendanceData.present_days} out of ${attendanceData.total_days} days (${attendancePercentage}%)`,
          15,
          infoY,
        );
        infoY += 6;
      }

      // Promotion Information
      if (reportCard.promoted_to_class_name) {
        doc.text(
          `Promoted to: ${reportCard.promoted_to_class_name}`,
          15,
          infoY,
        );
        infoY += 6;
      }

      // Student Interests
      if (reportCard.student_interest) {
        // Split long interest text into multiple lines
        const interestLines = doc.splitTextToSize(
          `Interests & Strengths: ${reportCard.student_interest}`,
          doc.internal.pageSize.width - 30,
        );
        doc.text(interestLines, 15, infoY);
        infoY += interestLines.length * 5 + 4;
      }

      // Add some space after the additional info section
      infoY += 8;
    }

    // 6. COMMENTS & REMARKS SECTION
    const commentsStartY = hasAdditionalInfo
      ? doc.lastAutoTable.finalY + (hasAdditionalInfo ? 40 : 20)
      : doc.lastAutoTable.finalY + 15;

    if (
      reportCard.teacher_comment ||
      reportCard.principal_comment ||
      reportCard.overall_comment
    ) {
      doc.setFontSize(12);
      doc.setFont("helvetica", "bold");
      doc.setTextColor(...primaryColor);
      doc.text("COMMENTS & REMARKS", 15, commentsStartY);

      let commentY = commentsStartY + 8;
      doc.setFontSize(10);
      doc.setFont("helvetica", "normal");
      doc.setTextColor(0, 0, 0);

      if (reportCard.teacher_comment) {
        const teacherLines = doc.splitTextToSize(
          `Teacher's Remarks: ${reportCard.teacher_comment}`,
          doc.internal.pageSize.width - 30,
        );
        doc.text(teacherLines, 15, commentY);
        commentY += teacherLines.length * 5 + 6;
      }

      if (reportCard.principal_comment) {
        const principalLines = doc.splitTextToSize(
          `Principal's Remarks: ${reportCard.principal_comment}`,
          doc.internal.pageSize.width - 30,
        );
        doc.text(principalLines, 15, commentY);
        commentY += principalLines.length * 5 + 6;
      }

      if (reportCard.overall_comment) {
        const overallLines = doc.splitTextToSize(
          `Overall Assessment: ${reportCard.overall_comment}`,
          doc.internal.pageSize.width - 30,
        );
        doc.text(overallLines, 15, commentY);
      }
    }

    // 7. FOOTER
    const footerY = doc.internal.pageSize.height - 20;

    doc.setFontSize(8);
    doc.setTextColor(100, 100, 100);
    doc.text(`Generated on: ${new Date().toLocaleDateString()}`, 15, footerY);

    if (reportCard.issued_by_name) {
      doc.text(
        `Signed: ${reportCard.issued_by_name}`,
        doc.internal.pageSize.width - 15,
        footerY,
        { align: "right" },
      );
    }

    doc.text(
      `${schoolSettings.school_short_name} - Confidential Student Report`,
      doc.internal.pageSize.width / 2,
      footerY + 8,
      { align: "center" },
    );

    // Get PDF as buffer and send/return
    const pdfBuffer = Buffer.from(doc.output("arraybuffer"));

    if (res) {
      res.send(pdfBuffer);
    }

    return pdfBuffer;
  } catch (error) {
    console.error("Error generating student report card PDF:", error);
    if (res) {
      res
        .status(500)
        .json({ error: "Failed to generate PDF: " + error.message });
    }
    throw error;
  }
};

// Helper function for ordinal suffixes
const getOrdinalSuffix = (position) => {
  if (position === 1) return "1st";
  if (position === 2) return "2nd";
  if (position === 3) return "3rd";
  return `${position}th`;
};

//attendance controller
// GET /api/attendance/class/:class_id - Get students in class for attendance
const getStudentsForAttendance = async (req, res) => {
  try {
    const { class_id } = req.params;
    const { date, academic_year_id, term_id } = req.query;

    // Use current academic year if not specified
    let academicYearId = academic_year_id;
    if (!academicYearId) {
      const [currentYear] = await pool.query(
        "SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1",
      );
      if (currentYear.length === 0) {
        return res.status(400).json({
          error:
            "No current academic year found. Please set a current academic year first.",
        });
      }
      academicYearId = currentYear[0].id;
    }

    // Get current term if not specified
    let termId = term_id;
    if (!termId) {
      const [currentTerm] = await pool.query(
        "SELECT id FROM terms WHERE start_date <= CURDATE() AND end_date >= CURDATE() LIMIT 1",
      );
      termId = currentTerm[0]?.id || 1; // Default to 1 if no current term
    }

    // Get students in the class
    const [students] = await pool.query(
      `
      SELECT 
        s.id,
        s.admission_number,
        s.first_name,
        s.last_name,
        s.gender,
        s.photo_filename,
        ca.promotion_status,
        ca.academic_year_id,  
        a.status as attendance_status,
        a.notes as attendance_notes,
        a.id as attendance_id
      FROM students s
      INNER JOIN class_assignments ca ON s.id = ca.student_id
      LEFT JOIN attendance a ON s.id = a.student_id 
        AND a.date = ? 
        AND a.academic_year_id = ?
        AND a.term_id = ?
      WHERE ca.class_id = ? 
        AND ca.academic_year_id = ?
        AND (s.is_active IS NULL OR s.is_active = TRUE)
      ORDER BY s.first_name, s.last_name
      `,
      [date, academicYearId, termId, class_id, academicYearId],
    );

    // Format response with default "Present" status
    const studentsWithAttendance = students.map((student) => ({
      ...student,
      attendance_status: student.attendance_status || "Present",
      attendance_notes: student.attendance_notes || "",
    }));

    // Also return the academic_year_id and term_id for frontend use
    res.json({
      students: studentsWithAttendance,
      academic_year_id: academicYearId,
      term_id: termId,
    });
  } catch (error) {
    console.error("Error fetching students for attendance:", error);
    res.status(500).json({ error: "Failed to fetch students for attendance" });
  }
};

// POST /api/attendance/mark - Mark attendance for a student
const markAttendance = async (req, res) => {
  try {
    const {
      student_id,
      academic_year_id,
      term_id,
      date,
      status,
      notes,
      recorded_by,
    } = req.body;

    // Validate required fields
    if (!student_id || !academic_year_id || !term_id || !date || !status) {
      return res.status(400).json({ error: "Missing required fields" });
    }

    // Check if attendance already exists for this student on this date
    const [existing] = await pool.query(
      "SELECT id FROM attendance WHERE student_id = ? AND date = ?",
      [student_id, date],
    );

    if (existing.length > 0) {
      // Update existing attendance
      await pool.query(
        "UPDATE attendance SET status = ?, notes = ?, recorded_by = ? WHERE student_id = ? AND date = ?",
        [status, notes, recorded_by, student_id, date],
      );
    } else {
      // Create new attendance record
      await pool.query(
        "INSERT INTO attendance (student_id, academic_year_id, term_id, date, status, notes, recorded_by) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
          student_id,
          academic_year_id,
          term_id,
          date,
          status,
          notes,
          recorded_by,
        ],
      );
    }

    res.json({
      message: "Attendance marked successfully",
      student_id,
      date,
      status,
    });
  } catch (error) {
    console.error("Error marking attendance:", error);
    res.status(500).json({ error: "Failed to mark attendance" });
  }
};

// POST /api/attendance/bulk - Mark attendance for multiple students
const markBulkAttendance = async (req, res) => {
  const connection = await pool.getConnection();

  try {
    await connection.beginTransaction();

    let { attendance_data, academic_year_id, term_id, date, recorded_by } =
      req.body;

    console.log("Bulk attendance data:", {
      attendance_data_count: attendance_data?.length,
      academic_year_id,
      term_id,
      date,
      recorded_by,
    });

    if (
      !attendance_data ||
      !Array.isArray(attendance_data) ||
      attendance_data.length === 0
    ) {
      await connection.rollback();
      return res.status(400).json({ error: "No attendance data provided" });
    }

    if (!date) {
      await connection.rollback();
      return res.status(400).json({ error: "Date is required" });
    }

    // FIX: Get academic_year_id if not provided
    if (!academic_year_id) {
      const [currentYear] = await connection.query(
        "SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1",
      );
      if (currentYear.length === 0) {
        await connection.rollback();
        return res.status(400).json({
          error:
            "No current academic year found. Please set a current academic year first.",
        });
      }
      academic_year_id = currentYear[0].id;
      console.log("Using academic_year_id from database:", academic_year_id);
    }

    // FIX: Get term_id if not provided
    if (!term_id) {
      const [currentTerm] = await connection.query(
        "SELECT id FROM terms WHERE start_date <= CURDATE() AND end_date >= CURDATE() LIMIT 1",
      );
      if (currentTerm.length > 0) {
        term_id = currentTerm[0].id;
        console.log("Using term_id from database:", term_id);
      } else {
        // Fallback to first term
        const [firstTerm] = await connection.query(
          "SELECT id FROM terms ORDER BY start_date LIMIT 1",
        );
        if (firstTerm.length > 0) {
          term_id = firstTerm[0].id;
          console.log("Using first term_id:", term_id);
        }
      }
    }

    const results = {
      success: 0,
      errors: [],
    };

    for (const record of attendance_data) {
      try {
        const { student_id, status, notes } = record;

        console.log("Processing attendance for student:", {
          student_id,
          status,
          notes,
          academic_year_id,
          term_id,
          date,
        });

        // Validate required fields
        if (!student_id || !status) {
          throw new Error("Missing student_id or status");
        }

        // Check if attendance already exists
        const [existing] = await connection.query(
          "SELECT id FROM attendance WHERE student_id = ? AND date = ? AND academic_year_id = ? AND term_id = ?",
          [student_id, date, academic_year_id, term_id],
        );

        if (existing.length > 0) {
          // Update existing
          await connection.query(
            "UPDATE attendance SET status = ?, notes = ?, recorded_by = ? WHERE student_id = ? AND date = ? AND academic_year_id = ? AND term_id = ?",
            [
              status,
              notes || "",
              recorded_by,
              student_id,
              date,
              academic_year_id,
              term_id,
            ],
          );
          console.log(`Updated attendance for student ${student_id}`);
        } else {
          // Insert new
          await connection.query(
            "INSERT INTO attendance (student_id, academic_year_id, term_id, date, status, notes, recorded_by) VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
              student_id,
              academic_year_id,
              term_id,
              date,
              status,
              notes || "",
              recorded_by,
            ],
          );
          console.log(`Created new attendance for student ${student_id}`);
        }

        results.success++;
      } catch (error) {
        console.error(`Error processing student ${record.student_id}:`, error);
        results.errors.push({
          student_id: record.student_id,
          error: error.message,
        });
      }
    }

    await connection.commit();

    console.log("Bulk attendance completed:", results);

    res.json({
      message: `Attendance marked for ${results.success} students`,
      success: results.success,
      errors: results.errors,
    });
  } catch (error) {
    await connection.rollback();
    console.error("Error in bulk attendance:", error);
    res
      .status(500)
      .json({ error: "Failed to mark bulk attendance: " + error.message });
  } finally {
    connection.release();
  }
};

// GET /api/attendance/records - Get attendance records with filters
const getAttendanceRecords = async (req, res) => {
  try {
    const {
      class_id,
      academic_year_id,
      term_id,
      start_date,
      end_date,
      student_id,
    } = req.query;

    let whereConditions = ["1=1"];
    let queryParams = [];

    if (class_id) {
      whereConditions.push(`
        a.student_id IN (
          SELECT student_id FROM class_assignments 
          WHERE class_id = ? AND academic_year_id = COALESCE(?, academic_year_id)
        )
      `);
      queryParams.push(class_id);
      if (academic_year_id) {
        queryParams.push(academic_year_id);
      } else {
        // Get current academic year if not provided
        const [currentYear] = await pool.query(
          "SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1",
        );
        if (currentYear.length > 0) {
          queryParams.push(currentYear[0].id);
        }
      }
    }

    if (academic_year_id) {
      whereConditions.push("a.academic_year_id = ?");
      queryParams.push(academic_year_id);
    } else {
      // Get current academic year if not provided
      const [currentYear] = await pool.query(
        "SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1",
      );
      if (currentYear.length > 0) {
        whereConditions.push("a.academic_year_id = ?");
        queryParams.push(currentYear[0].id);
      }
    }

    if (term_id) {
      whereConditions.push("a.term_id = ?");
      queryParams.push(term_id);
    }

    if (start_date) {
      whereConditions.push("a.date >= ?");
      queryParams.push(start_date);
    }

    if (end_date) {
      whereConditions.push("a.date <= ?");
      queryParams.push(end_date);
    }

    if (student_id) {
      whereConditions.push("a.student_id = ?");
      queryParams.push(student_id);
    }

    const [records] = await pool.query(
      `
      SELECT 
        a.*,
        s.first_name,
        s.last_name,
        s.admission_number,
        c.class_name,
        ay.year_label as academic_year,
        t.term_name,
        u.username as recorded_by_name
      FROM attendance a
      INNER JOIN students s ON a.student_id = s.id
      LEFT JOIN class_assignments ca ON s.id = ca.student_id AND a.academic_year_id = ca.academic_year_id
      LEFT JOIN classes c ON ca.class_id = c.id
      INNER JOIN academic_years ay ON a.academic_year_id = ay.id
      INNER JOIN terms t ON a.term_id = t.id
      LEFT JOIN users u ON a.recorded_by = u.id
      WHERE ${whereConditions.join(" AND ")}
      ORDER BY a.date DESC, s.first_name, s.last_name
      `,
      queryParams,
    );

    res.json(records);
  } catch (error) {
    console.error("Error fetching attendance records:", error);
    res.status(500).json({ error: "Failed to fetch attendance records" });
  }
};

// GET /api/attendance/statistics - Get attendance statistics
const getAttendanceStatistics = async (req, res) => {
  try {
    const { class_id, academic_year_id, term_id, start_date, end_date } =
      req.query;

    let whereConditions = ["1=1"];
    let queryParams = [];

    if (class_id) {
      whereConditions.push(`
        a.student_id IN (
          SELECT student_id FROM class_assignments 
          WHERE class_id = ? AND academic_year_id = COALESCE(?, academic_year_id)
        )
      `);
      queryParams.push(class_id);
      if (academic_year_id) {
        queryParams.push(academic_year_id);
      }
    }

    if (academic_year_id) {
      whereConditions.push("a.academic_year_id = ?");
      queryParams.push(academic_year_id);
    }

    if (term_id) {
      whereConditions.push("a.term_id = ?");
      queryParams.push(term_id);
    }

    if (start_date) {
      whereConditions.push("a.date >= ?");
      queryParams.push(start_date);
    }

    if (end_date) {
      whereConditions.push("a.date <= ?");
      queryParams.push(end_date);
    }

    const [stats] = await pool.query(
      `
      SELECT 
        COUNT(*) as total_records,
        SUM(CASE WHEN status = 'Present' THEN 1 ELSE 0 END) as present_count,
        SUM(CASE WHEN status = 'Absent' THEN 1 ELSE 0 END) as absent_count,
        SUM(CASE WHEN status = 'Late' THEN 1 ELSE 0 END) as late_count,
        SUM(CASE WHEN status = 'Excused' THEN 1 ELSE 0 END) as excused_count,
        COUNT(DISTINCT student_id) as total_students,
        COUNT(DISTINCT date) as total_days
      FROM attendance a
      WHERE ${whereConditions.join(" AND ")}
      `,
      queryParams,
    );

    const statistics = stats[0] || {};
    const presentPercentage =
      statistics.total_records > 0
        ? Math.round(
            (statistics.present_count / statistics.total_records) * 100,
          )
        : 0;

    res.json({
      ...statistics,
      present_percentage: presentPercentage,
    });
  } catch (error) {
    console.error("Error fetching attendance statistics:", error);
    res.status(500).json({ error: "Failed to fetch attendance statistics" });
  }
};

// GET /api/attendance/reports - Generate attendance reports
const getAttendanceReports = async (req, res) => {
  try {
    const {
      class_id,
      academic_year_id,
      term_id,
      report_type,
      start_date,
      end_date,
      month,
      year,
    } = req.query;

    if (!class_id) {
      return res.status(400).json({ error: "Class ID is required" });
    }

    // Get class information
    const [classInfo] = await pool.query(
      "SELECT class_name FROM classes WHERE id = ?",
      [class_id],
    );

    if (classInfo.length === 0) {
      return res.status(404).json({ error: "Class not found" });
    }

    const className = classInfo[0].class_name;
    let reportData = {
      report_title: "",
      report_period: "",
      class_name: className,
      total_students: 0,
      total_days: 0,
    };

    // Get total students in class
    const [studentCount] = await pool.query(
      `SELECT COUNT(*) as count FROM students s
       INNER JOIN class_assignments ca ON s.id = ca.student_id
       WHERE ca.class_id = ? AND ca.academic_year_id = ?`,
      [class_id, academic_year_id],
    );
    reportData.total_students = studentCount[0].count;

    if (report_type === "student_wise") {
      reportData = await generateStudentWiseReport(
        reportData,
        class_id,
        academic_year_id,
        term_id,
        start_date,
        end_date,
      );
    } else if (report_type === "daily_summary") {
      reportData = await generateDailySummaryReport(
        reportData,
        class_id,
        academic_year_id,
        term_id,
        start_date,
        end_date,
      );
    } else if (report_type === "monthly_summary") {
      reportData = await generateMonthlySummaryReport(
        reportData,
        class_id,
        academic_year_id,
        term_id,
        month,
        year,
      );
    } else if (report_type === "trend_analysis") {
      reportData = await generateTrendAnalysisReport(
        reportData,
        class_id,
        academic_year_id,
        term_id,
        start_date,
        end_date,
      );
    } else {
      return res.status(400).json({ error: "Invalid report type" });
    }

    res.json(reportData);
  } catch (error) {
    console.error("Error generating attendance report:", error);
    res.status(500).json({ error: "Failed to generate attendance report" });
  }
};

// Helper function for trend analysis report
const generateTrendAnalysisReport = async (
  reportData,
  class_id,
  academic_year_id,
  term_id,
  start_date,
  end_date,
) => {
  let whereConditions = ["ca.class_id = ?", "ca.academic_year_id = ?"];
  let queryParams = [class_id, academic_year_id];

  if (term_id) {
    whereConditions.push("a.term_id = ?");
    queryParams.push(term_id);
  }

  if (start_date && end_date) {
    whereConditions.push("a.date BETWEEN ? AND ?");
    queryParams.push(start_date, end_date);
    reportData.report_period = `${start_date} to ${end_date}`;
  } else {
    // Default to last 30 days if no date range provided
    const defaultEndDate = new Date().toISOString().split("T")[0];
    const defaultStartDate = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000)
      .toISOString()
      .split("T")[0];
    whereConditions.push("a.date BETWEEN ? AND ?");
    queryParams.push(defaultStartDate, defaultEndDate);
    reportData.report_period = `${defaultStartDate} to ${defaultEndDate}`;
  }

  reportData.report_title = "Attendance Trend Analysis Report";

  // Get weekly trends
  const [weeklyTrends] = await pool.query(
    `
    SELECT 
      YEARWEEK(a.date) as week_number,
      MIN(a.date) as week_start,
      MAX(a.date) as week_end,
      COUNT(a.id) as total_records,
      SUM(CASE WHEN a.status = 'Present' THEN 1 ELSE 0 END) as present_count,
      SUM(CASE WHEN a.status = 'Absent' THEN 1 ELSE 0 END) as absent_count,
      SUM(CASE WHEN a.status = 'Late' THEN 1 ELSE 0 END) as late_count,
      SUM(CASE WHEN a.status = 'Excused' THEN 1 ELSE 0 END) as excused_count,
      ROUND((SUM(CASE WHEN a.status = 'Present' THEN 1 ELSE 0 END) * 100.0 / COUNT(a.id)), 2) as attendance_rate
    FROM attendance a
    INNER JOIN students s ON a.student_id = s.id
    INNER JOIN class_assignments ca ON s.id = ca.student_id
    WHERE ${whereConditions.join(" AND ")}
    GROUP BY YEARWEEK(a.date)
    ORDER BY week_number
    `,
    queryParams,
  );

  // Get monthly trends
  const [monthlyTrends] = await pool.query(
    `
    SELECT 
      DATE_FORMAT(a.date, '%Y-%m') as month_year,
      DATE_FORMAT(a.date, '%M %Y') as month_name,
      COUNT(a.id) as total_records,
      SUM(CASE WHEN a.status = 'Present' THEN 1 ELSE 0 END) as present_count,
      SUM(CASE WHEN a.status = 'Absent' THEN 1 ELSE 0 END) as absent_count,
      SUM(CASE WHEN a.status = 'Late' THEN 1 ELSE 0 END) as late_count,
      SUM(CASE WHEN a.status = 'Excused' THEN 1 ELSE 0 END) as excused_count,
      ROUND((SUM(CASE WHEN a.status = 'Present' THEN 1 ELSE 0 END) * 100.0 / COUNT(a.id)), 2) as attendance_rate
    FROM attendance a
    INNER JOIN students s ON a.student_id = s.id
    INNER JOIN class_assignments ca ON s.id = ca.student_id
    WHERE ${whereConditions.join(" AND ")}
    GROUP BY DATE_FORMAT(a.date, '%Y-%m'), DATE_FORMAT(a.date, '%M %Y')
    ORDER BY month_year
    `,
    queryParams,
  );

  // Get day-of-week trends
  const [dayOfWeekTrends] = await pool.query(
    `
    SELECT 
      DAYNAME(a.date) as day_name,
      DAYOFWEEK(a.date) as day_number,
      COUNT(a.id) as total_records,
      SUM(CASE WHEN a.status = 'Present' THEN 1 ELSE 0 END) as present_count,
      SUM(CASE WHEN a.status = 'Absent' THEN 1 ELSE 0 END) as absent_count,
      SUM(CASE WHEN a.status = 'Late' THEN 1 ELSE 0 END) as late_count,
      SUM(CASE WHEN a.status = 'Excused' THEN 1 ELSE 0 END) as excused_count,
      ROUND((SUM(CASE WHEN a.status = 'Present' THEN 1 ELSE 0 END) * 100.0 / COUNT(a.id)), 2) as attendance_rate
    FROM attendance a
    INNER JOIN students s ON a.student_id = s.id
    INNER JOIN class_assignments ca ON s.id = ca.student_id
    WHERE ${whereConditions.join(" AND ")}
    GROUP BY DAYNAME(a.date), DAYOFWEEK(a.date)
    ORDER BY day_number
    `,
    queryParams,
  );

  // Get student attendance trends (improving vs declining)
  const [studentTrends] = await pool.query(
    `
    WITH student_attendance AS (
      SELECT 
        s.id as student_id,
        s.first_name,
        s.last_name,
        s.admission_number,
        a.date,
        a.status,
        WEEK(a.date) as week_number,
        CASE WHEN a.status = 'Present' THEN 1 ELSE 0 END as is_present
      FROM students s
      INNER JOIN class_assignments ca ON s.id = ca.student_id
      INNER JOIN attendance a ON s.id = a.student_id
      WHERE ${whereConditions.join(" AND ")}
    ),
    weekly_attendance AS (
      SELECT 
        student_id,
        first_name,
        last_name,
        admission_number,
        week_number,
        AVG(is_present) * 100 as weekly_attendance_rate
      FROM student_attendance
      GROUP BY student_id, first_name, last_name, admission_number, week_number
    ),
    trend_analysis AS (
      SELECT 
        student_id,
        first_name,
        last_name,
        admission_number,
        COUNT(*) as total_weeks,
        AVG(weekly_attendance_rate) as avg_attendance,
        MAX(weekly_attendance_rate) - MIN(weekly_attendance_rate) as trend_change,
        CASE 
          WHEN MAX(weekly_attendance_rate) - MIN(weekly_attendance_rate) > 10 THEN 'Improving'
          WHEN MAX(weekly_attendance_rate) - MIN(weekly_attendance_rate) < -10 THEN 'Declining'
          ELSE 'Stable'
        END as trend_direction
      FROM weekly_attendance
      GROUP BY student_id, first_name, last_name, admission_number
      HAVING total_weeks >= 2
    )
    SELECT * FROM trend_analysis
    ORDER BY trend_change DESC
    `,
    queryParams,
  );

  reportData.trend_analysis = {
    weekly_trends: weeklyTrends,
    monthly_trends: monthlyTrends,
    day_of_week_trends: dayOfWeekTrends,
    student_trends: studentTrends,
  };

  // Calculate summary statistics for trends
  const totalWeeks = weeklyTrends.length;
  const totalMonths = monthlyTrends.length;

  if (totalWeeks > 0) {
    const firstWeekRate = weeklyTrends[0].attendance_rate;
    const lastWeekRate = weeklyTrends[totalWeeks - 1].attendance_rate;
    const overallTrend = lastWeekRate - firstWeekRate;

    reportData.summary = {
      analysis_period: `${totalWeeks} weeks, ${totalMonths} months`,
      overall_trend:
        overallTrend > 0
          ? "Improving"
          : overallTrend < 0
            ? "Declining"
            : "Stable",
      trend_change: Math.abs(overallTrend).toFixed(2) + "%",
      best_day: dayOfWeekTrends.reduce((best, day) =>
        day.attendance_rate > best.attendance_rate ? day : best,
      ),
      worst_day: dayOfWeekTrends.reduce((worst, day) =>
        day.attendance_rate < worst.attendance_rate ? day : worst,
      ),
      improving_students: studentTrends.filter(
        (s) => s.trend_direction === "Improving",
      ).length,
      declining_students: studentTrends.filter(
        (s) => s.trend_direction === "Declining",
      ).length,
      stable_students: studentTrends.filter(
        (s) => s.trend_direction === "Stable",
      ).length,
    };
  }

  reportData.total_days = await getTotalDays(
    class_id,
    academic_year_id,
    term_id,
    start_date,
    end_date,
  );

  return reportData;
};

// Helper function for student-wise report
const generateStudentWiseReport = async (
  reportData,
  class_id,
  academic_year_id,
  term_id,
  start_date,
  end_date,
) => {
  let whereConditions = ["ca.class_id = ?", "ca.academic_year_id = ?"];
  let queryParams = [class_id, academic_year_id];

  if (term_id) {
    whereConditions.push("a.term_id = ?");
    queryParams.push(term_id);
  }

  if (start_date && end_date) {
    whereConditions.push("a.date BETWEEN ? AND ?");
    queryParams.push(start_date, end_date);
    reportData.report_period = `${start_date} to ${end_date}`;
  }

  reportData.report_title = "Student-wise Attendance Report";

  // Get student attendance summary
  const [students] = await pool.query(
    `
    SELECT 
      s.id,
      s.first_name,
      s.last_name,
      s.admission_number,
      COUNT(a.id) as total_records,
      SUM(CASE WHEN a.status = 'Present' THEN 1 ELSE 0 END) as present_count,
      SUM(CASE WHEN a.status = 'Absent' THEN 1 ELSE 0 END) as absent_count,
      SUM(CASE WHEN a.status = 'Late' THEN 1 ELSE 0 END) as late_count,
      SUM(CASE WHEN a.status = 'Excused' THEN 1 ELSE 0 END) as excused_count
    FROM students s
    INNER JOIN class_assignments ca ON s.id = ca.student_id
    LEFT JOIN attendance a ON s.id = a.student_id AND ${whereConditions.join(
      " AND ",
    )}
    WHERE ca.class_id = ? AND ca.academic_year_id = ?
    GROUP BY s.id, s.first_name, s.last_name, s.admission_number
    ORDER BY s.first_name, s.last_name
    `,
    [...queryParams, class_id, academic_year_id],
  );

  // Calculate percentages
  const studentsWithPercentage = students.map((student) => {
    const total = student.total_records || 1; // Avoid division by zero
    const attendance_percentage = Math.round(
      (student.present_count / total) * 100,
    );
    return {
      ...student,
      attendance_percentage: isNaN(attendance_percentage)
        ? 0
        : attendance_percentage,
    };
  });

  reportData.students = studentsWithPercentage;
  reportData.total_days = await getTotalDays(
    class_id,
    academic_year_id,
    term_id,
    start_date,
    end_date,
  );

  // Add summary statistics
  reportData.summary = {
    present_percentage: Math.round(
      studentsWithPercentage.reduce(
        (sum, student) => sum + student.attendance_percentage,
        0,
      ) / studentsWithPercentage.length,
    ),
    average_daily_attendance: await getAverageDailyAttendance(
      class_id,
      academic_year_id,
      term_id,
      start_date,
      end_date,
    ),
    most_absent_student_count: Math.max(
      ...studentsWithPercentage.map((s) => s.absent_count),
    ),
    perfect_attendance_count: studentsWithPercentage.filter(
      (s) => s.attendance_percentage === 100,
    ).length,
  };

  return reportData;
};

// Helper function for daily summary report
const generateDailySummaryReport = async (
  reportData,
  class_id,
  academic_year_id,
  term_id,
  start_date,
  end_date,
) => {
  let whereConditions = [
    "ca.class_id = ?",
    "ca.academic_year_id = ?",
    "a.date BETWEEN ? AND ?",
  ];
  let queryParams = [class_id, academic_year_id, start_date, end_date];

  if (term_id) {
    whereConditions.push("a.term_id = ?");
    queryParams.push(term_id);
  }

  reportData.report_title = "Daily Attendance Summary Report";
  reportData.report_period = `${start_date} to ${end_date}`;

  // Get daily attendance summary
  const [dailySummary] = await pool.query(
    `
    SELECT 
      a.date,
      COUNT(a.id) as total_records,
      SUM(CASE WHEN a.status = 'Present' THEN 1 ELSE 0 END) as present_count,
      SUM(CASE WHEN a.status = 'Absent' THEN 1 ELSE 0 END) as absent_count,
      SUM(CASE WHEN a.status = 'Late' THEN 1 ELSE 0 END) as late_count,
      SUM(CASE WHEN a.status = 'Excused' THEN 1 ELSE 0 END) as excused_count,
      ROUND((SUM(CASE WHEN a.status = 'Present' THEN 1 ELSE 0 END) * 100.0 / COUNT(a.id)), 2) as attendance_rate
    FROM attendance a
    INNER JOIN students s ON a.student_id = s.id
    INNER JOIN class_assignments ca ON s.id = ca.student_id
    WHERE ${whereConditions.join(" AND ")}
    GROUP BY a.date
    ORDER BY a.date DESC
    `,
    queryParams,
  );

  reportData.daily_summary = dailySummary;
  reportData.total_days = dailySummary.length;

  return reportData;
};

// Helper function for monthly summary report
const generateMonthlySummaryReport = async (
  reportData,
  class_id,
  academic_year_id,
  term_id,
  month,
  year,
) => {
  const start_date = `${year}-${month.toString().padStart(2, "0")}-01`;
  const end_date = new Date(year, month, 0).toISOString().split("T")[0]; // Last day of month

  reportData.report_title = "Monthly Attendance Summary Report";
  reportData.report_period = `${new Date(2000, month - 1).toLocaleString(
    "default",
    { month: "long" },
  )} ${year}`;

  // Get student attendance for the month
  const [students] = await pool.query(
    `
    SELECT 
      s.id,
      s.first_name,
      s.last_name,
      COUNT(a.id) as total_days,
      SUM(CASE WHEN a.status = 'Present' THEN 1 ELSE 0 END) as present_count,
      ROUND((SUM(CASE WHEN a.status = 'Present' THEN 1 ELSE 0 END) * 100.0 / COUNT(a.id)), 2) as attendance_percentage
    FROM students s
    INNER JOIN class_assignments ca ON s.id = ca.student_id
    LEFT JOIN attendance a ON s.id = a.student_id 
      AND a.academic_year_id = ? 
      AND a.term_id = ?
      AND a.date BETWEEN ? AND ?
    WHERE ca.class_id = ? AND ca.academic_year_id = ?
    GROUP BY s.id, s.first_name, s.last_name
    `,
    [
      academic_year_id,
      term_id,
      start_date,
      end_date,
      class_id,
      academic_year_id,
    ],
  );

  const totalDays = await getTotalDays(
    class_id,
    academic_year_id,
    term_id,
    start_date,
    end_date,
  );

  reportData.monthly_summary = {
    total_days: totalDays,
    avg_daily_attendance: await getAverageDailyAttendance(
      class_id,
      academic_year_id,
      term_id,
      start_date,
      end_date,
    ),
    perfect_attendance_count: students.filter(
      (s) => s.present_count === totalDays,
    ).length,
    low_attendance_count: students.filter((s) => s.attendance_percentage < 75)
      .length,
    distribution: {
      excellent: students.filter((s) => s.attendance_percentage >= 90).length,
      good: students.filter(
        (s) => s.attendance_percentage >= 75 && s.attendance_percentage < 90,
      ).length,
      poor: students.filter((s) => s.attendance_percentage < 75).length,
    },
  };

  reportData.total_days = totalDays;
  return reportData;
};

// Helper functions
const getTotalDays = async (
  class_id,
  academic_year_id,
  term_id,
  start_date,
  end_date,
) => {
  let whereConditions = ["ca.class_id = ?", "ca.academic_year_id = ?"];
  let queryParams = [class_id, academic_year_id];

  if (term_id) {
    whereConditions.push("a.term_id = ?");
    queryParams.push(term_id);
  }

  if (start_date && end_date) {
    whereConditions.push("a.date BETWEEN ? AND ?");
    queryParams.push(start_date, end_date);
  }

  const [result] = await pool.query(
    `SELECT COUNT(DISTINCT a.date) as total_days
     FROM attendance a
     INNER JOIN students s ON a.student_id = s.id
     INNER JOIN class_assignments ca ON s.id = ca.student_id
     WHERE ${whereConditions.join(" AND ")}`,
    queryParams,
  );

  return result[0]?.total_days || 0;
};

const getAverageDailyAttendance = async (
  class_id,
  academic_year_id,
  term_id,
  start_date,
  end_date,
) => {
  let whereConditions = ["ca.class_id = ?", "ca.academic_year_id = ?"];
  let queryParams = [class_id, academic_year_id];

  if (term_id) {
    whereConditions.push("a.term_id = ?");
    queryParams.push(term_id);
  }

  if (start_date && end_date) {
    whereConditions.push("a.date BETWEEN ? AND ?");
    queryParams.push(start_date, end_date);
  }

  const [result] = await pool.query(
    `SELECT 
       AVG(daily_attendance.attendance_rate) as avg_attendance
     FROM (
       SELECT 
         a.date,
         ROUND((SUM(CASE WHEN a.status = 'Present' THEN 1 ELSE 0 END) * 100.0 / COUNT(a.id)), 2) as attendance_rate
       FROM attendance a
       INNER JOIN students s ON a.student_id = s.id
       INNER JOIN class_assignments ca ON s.id = ca.student_id
       WHERE ${whereConditions.join(" AND ")}
       GROUP BY a.date
     ) as daily_attendance`,
    queryParams,
  );

  return Math.round(result[0]?.avg_attendance || 0);
};

// GET /api/attendance/export - Export attendance reports - UPDATED
const exportAttendanceReport = async (req, res) => {
  try {
    const {
      class_id,
      academic_year_id,
      term_id,
      report_type,
      start_date,
      end_date,
      month,
      year,
      format,
    } = req.query;

    if (!class_id) {
      return res.status(400).json({ error: "Class ID is required" });
    }

    console.log("Export request received:", {
      class_id,
      academic_year_id,
      term_id,
      report_type,
      start_date,
      end_date,
      month,
      year,
      format,
    });

    // Get report data by calling the report generation function directly
    let reportData;
    try {
      // Create a mock response object to capture the report data
      const mockRes = {
        json: (data) => {
          reportData = data;
          return data;
        },
      };

      // Generate the report data
      await getAttendanceReports({ query: req.query }, mockRes);

      if (!reportData) {
        throw new Error("Failed to generate report data");
      }

      console.log("Report data generated successfully:", {
        hasStudents: !!reportData.students,
        studentCount: reportData.students ? reportData.students.length : 0,
        hasDailySummary: !!reportData.daily_summary,
        dailySummaryCount: reportData.daily_summary
          ? reportData.daily_summary.length
          : 0,
        reportTitle: reportData.report_title,
        className: reportData.class_name,
      });
    } catch (reportError) {
      console.error("Error generating report for export:", reportError);
      return res.status(500).json({
        error: "Failed to generate report data: " + reportError.message,
      });
    }

    // Export based on format
    if (format === "excel") {
      await exportToExcel(reportData, res);
    } else if (format === "pdf") {
      await exportToPDF(reportData, res);
    } else {
      return res
        .status(400)
        .json({ error: "Invalid format. Use 'excel' or 'pdf'" });
    }
  } catch (error) {
    console.error("Error exporting attendance report:", error);
    res
      .status(500)
      .json({ error: "Failed to export attendance report: " + error.message });
  }
};

// Excel Export Helper - UPDATED
const exportToExcel = async (reportData, res) => {
  const XLSX = require("xlsx");

  const workbook = XLSX.utils.book_new();

  // Check if reportData has the expected structure
  if (!reportData) {
    throw new Error("No report data available for export");
  }

  // Student-wise report export
  if (reportData.students && Array.isArray(reportData.students)) {
    const worksheetData = reportData.students.map((student) => ({
      "Admission Number": student.admission_number || "",
      "Student Name": `${student.first_name || ""} ${
        student.last_name || ""
      }`.trim(),
      Present: student.present_count || 0,
      Absent: student.absent_count || 0,
      Late: student.late_count || 0,
      Excused: student.excused_count || 0,
      "Attendance Percentage": `${student.attendance_percentage || 0}%`,
      Status:
        student.attendance_percentage >= 90
          ? "Excellent"
          : student.attendance_percentage >= 75
            ? "Good"
            : "Needs Improvement",
    }));

    const worksheet = XLSX.utils.json_to_sheet(worksheetData);
    XLSX.utils.book_append_sheet(workbook, worksheet, "Student Attendance");
  }

  // Daily summary report export
  if (reportData.daily_summary && Array.isArray(reportData.daily_summary)) {
    const dailyData = reportData.daily_summary.map((day) => ({
      Date: day.date ? new Date(day.date).toLocaleDateString() : "",
      Present: day.present_count || 0,
      Absent: day.absent_count || 0,
      Late: day.late_count || 0,
      Excused: day.excused_count || 0,
      "Attendance Rate": `${day.attendance_rate || 0}%`,
    }));

    const dailyWorksheet = XLSX.utils.json_to_sheet(dailyData);
    XLSX.utils.book_append_sheet(workbook, dailyWorksheet, "Daily Summary");
  }

  // Trend analysis export
  if (reportData.trend_analysis) {
    // Weekly trends
    if (
      reportData.trend_analysis.weekly_trends &&
      Array.isArray(reportData.trend_analysis.weekly_trends)
    ) {
      const weeklyData = reportData.trend_analysis.weekly_trends.map(
        (week) => ({
          Week: `Week ${week.week_number || ""}`,
          Period: `${
            week.week_start
              ? new Date(week.week_start).toLocaleDateString()
              : ""
          } - ${
            week.week_end ? new Date(week.week_end).toLocaleDateString() : ""
          }`,
          Present: week.present_count || 0,
          Absent: week.absent_count || 0,
          Late: week.late_count || 0,
          Excused: week.excused_count || 0,
          "Attendance Rate": `${week.attendance_rate || 0}%`,
        }),
      );

      const weeklyWorksheet = XLSX.utils.json_to_sheet(weeklyData);
      XLSX.utils.book_append_sheet(workbook, weeklyWorksheet, "Weekly Trends");
    }

    // Student trends
    if (
      reportData.trend_analysis.student_trends &&
      Array.isArray(reportData.trend_analysis.student_trends)
    ) {
      const studentTrendsData = reportData.trend_analysis.student_trends.map(
        (student) => ({
          "Student Name": `${student.first_name || ""} ${
            student.last_name || ""
          }`.trim(),
          "Admission Number": student.admission_number || "",
          "Average Attendance": `${(student.avg_attendance || 0).toFixed(1)}%`,
          "Trend Change": `${(student.trend_change || 0).toFixed(1)}%`,
          "Trend Direction": student.trend_direction || "Stable",
        }),
      );

      const trendsWorksheet = XLSX.utils.json_to_sheet(studentTrendsData);
      XLSX.utils.book_append_sheet(workbook, trendsWorksheet, "Student Trends");
    }
  }

  // Add summary sheet with safe property access
  const summaryData = [
    {
      Metric: "Report Title",
      Value: reportData.report_title || "Attendance Report",
    },
    { Metric: "Class", Value: reportData.class_name || "Unknown Class" },
    {
      Metric: "Report Period",
      Value: reportData.report_period || "Not specified",
    },
    { Metric: "Total Students", Value: reportData.total_students || 0 },
    { Metric: "Total Days", Value: reportData.total_days || 0 },
  ];

  if (reportData.summary) {
    summaryData.push(
      {
        Metric: "Overall Attendance Rate",
        Value: reportData.summary.present_percentage
          ? `${reportData.summary.present_percentage}%`
          : "N/A",
      },
      {
        Metric: "Average Daily Attendance",
        Value: reportData.summary.average_daily_attendance || "N/A",
      },
      {
        Metric: "Perfect Attendance Count",
        Value: reportData.summary.perfect_attendance_count || 0,
      },
    );
  }

  // Add trend analysis summary if available
  if (reportData.trend_analysis && reportData.summary) {
    summaryData.push(
      {
        Metric: "Overall Trend",
        Value: reportData.summary.overall_trend || "N/A",
      },
      {
        Metric: "Improving Students",
        Value: reportData.summary.improving_students || 0,
      },
      {
        Metric: "Declining Students",
        Value: reportData.summary.declining_students || 0,
      },
      {
        Metric: "Stable Students",
        Value: reportData.summary.stable_students || 0,
      },
    );
  }

  const summaryWorksheet = XLSX.utils.json_to_sheet(summaryData);
  XLSX.utils.book_append_sheet(workbook, summaryWorksheet, "Summary");

  const buffer = XLSX.write(workbook, { type: "buffer", bookType: "xlsx" });

  res.setHeader(
    "Content-Type",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  );
  res.setHeader(
    "Content-Disposition",
    `attachment; filename="attendance-report-${
      reportData.class_name || "unknown"
    }.xlsx"`,
  );
  res.send(buffer);
};

// PDF Export Helper - UPDATED
const exportToPDF = async (reportData, res) => {
  const { jsPDF } = require("jspdf");
  const { autoTable } = require("jspdf-autotable");

  const doc = new jsPDF();

  // Safe property access with defaults
  const reportTitle = reportData.report_title || "Attendance Report";
  const className = reportData.class_name || "Unknown Class";
  const reportPeriod = reportData.report_period || "Not specified";

  // Add title with safe values
  doc.setFontSize(16);
  doc.text(reportTitle, 20, 20);
  doc.setFontSize(12);
  doc.text(`Class: ${className}`, 20, 30);
  doc.text(`Period: ${reportPeriod}`, 20, 40);
  doc.text(`Generated: ${new Date().toLocaleDateString()}`, 20, 50);

  let startY = 70;

  // Student-wise report
  if (reportData.students && Array.isArray(reportData.students)) {
    const tableData = reportData.students.map((student) => [
      student.admission_number || "",
      `${student.first_name || ""} ${student.last_name || ""}`.trim(),
      student.present_count || 0,
      student.absent_count || 0,
      student.late_count || 0,
      student.excused_count || 0,
      `${student.attendance_percentage || 0}%`,
    ]);

    autoTable(doc, {
      startY: startY,
      head: [
        [
          "Admission No",
          "Student Name",
          "Present",
          "Absent",
          "Late",
          "Excused",
          "Attendance %",
        ],
      ],
      body: tableData,
      headStyles: {
        fillColor: [41, 128, 185],
        textColor: [255, 255, 255],
        fontStyle: "bold",
      },
      styles: {
        fontSize: 8,
        cellPadding: 2,
      },
    });

    startY = doc.lastAutoTable.finalY + 10;
  }

  // Daily summary report
  if (reportData.daily_summary && Array.isArray(reportData.daily_summary)) {
    // Add new page if needed
    if (startY > 200) {
      doc.addPage();
      startY = 20;
    }

    doc.setFontSize(14);
    doc.text("Daily Summary", 20, startY);
    startY += 10;

    const dailyTableData = reportData.daily_summary.map((day) => [
      day.date ? new Date(day.date).toLocaleDateString() : "",
      day.present_count || 0,
      day.absent_count || 0,
      day.late_count || 0,
      day.excused_count || 0,
      `${day.attendance_rate || 0}%`,
    ]);

    autoTable(doc, {
      startY: startY,
      head: [
        ["Date", "Present", "Absent", "Late", "Excused", "Attendance Rate"],
      ],
      body: dailyTableData,
      headStyles: {
        fillColor: [41, 128, 185],
        textColor: [255, 255, 255],
        fontStyle: "bold",
      },
      styles: {
        fontSize: 8,
        cellPadding: 2,
      },
    });

    startY = doc.lastAutoTable.finalY + 10;
  }

  // Add summary information
  if (startY > 250) {
    doc.addPage();
    startY = 20;
  }

  doc.setFontSize(12);
  doc.text("Summary Information", 20, startY);
  startY += 10;

  doc.setFontSize(10);
  const summaryLines = [
    `Total Students: ${reportData.total_students || 0}`,
    `Total Days: ${reportData.total_days || 0}`,
  ];

  if (reportData.summary) {
    if (reportData.summary.present_percentage) {
      summaryLines.push(
        `Overall Attendance: ${reportData.summary.present_percentage}%`,
      );
    }
    if (reportData.summary.average_daily_attendance) {
      summaryLines.push(
        `Average Daily Attendance: ${reportData.summary.average_daily_attendance}`,
      );
    }
    if (reportData.summary.perfect_attendance_count !== undefined) {
      summaryLines.push(
        `Perfect Attendance: ${reportData.summary.perfect_attendance_count} students`,
      );
    }
  }

  summaryLines.forEach((line, index) => {
    doc.text(line, 20, startY + index * 5);
  });

  res.setHeader("Content-Type", "application/pdf");
  res.setHeader(
    "Content-Disposition",
    `attachment; filename="attendance-report-${className}.pdf"`,
  );
  res.send(Buffer.from(doc.output("arraybuffer")));
};

//fee controllers

// GET /api/fee-categories - Get all fee categories
const getFeeCategories = async (req, res) => {
  try {
    const [categories] = await pool.query(`
      SELECT * FROM fee_categories 
      ORDER BY category_name
    `);
    res.json(categories);
  } catch (error) {
    console.error("Error fetching fee categories:", error);
    res.status(500).json({ error: "Failed to fetch fee categories" });
  }
};

// POST /api/fee-categories - Create new fee category
const createFeeCategory = async (req, res) => {
  try {
    const { category_name, description } = req.body;

    // Check if category name already exists
    const [existing] = await pool.query(
      "SELECT id FROM fee_categories WHERE category_name = ?",
      [category_name],
    );

    if (existing.length > 0) {
      return res
        .status(400)
        .json({ error: "Fee category name already exists" });
    }

    const [result] = await pool.query(
      "INSERT INTO fee_categories (category_name, description) VALUES (?, ?)",
      [category_name, description],
    );
    clearRelevantCaches("UPDATE_FEE_CATEGORY");

    const [newCategory] = await pool.query(
      "SELECT * FROM fee_categories WHERE id = ?",
      [result.insertId],
    );

    res.status(201).json(newCategory[0]);
  } catch (error) {
    console.error("Error creating fee category:", error);
    res.status(500).json({ error: "Failed to create fee category" });
  }
};

// PUT /api/fee-categories/:id - Update fee category
const updateFeeCategory = async (req, res) => {
  try {
    const { category_name, description } = req.body;

    // Check if category exists
    const [existing] = await pool.query(
      "SELECT id FROM fee_categories WHERE id = ?",
      [req.params.id],
    );

    if (existing.length === 0) {
      return res.status(404).json({ error: "Fee category not found" });
    }

    // Check if category name is taken by another category
    const [nameCheck] = await pool.query(
      "SELECT id FROM fee_categories WHERE category_name = ? AND id != ?",
      [category_name, req.params.id],
    );

    if (nameCheck.length > 0) {
      return res
        .status(400)
        .json({ error: "Fee category name already exists" });
    }

    await pool.query(
      "UPDATE fee_categories SET category_name = ?, description = ? WHERE id = ?",
      [category_name, description, req.params.id],
    );
    clearRelevantCaches("UPDATE_FEE_CATEGORY");

    const [updatedCategory] = await pool.query(
      "SELECT * FROM fee_categories WHERE id = ?",
      [req.params.id],
    );

    res.json(updatedCategory[0]);
  } catch (error) {
    console.error("Error updating fee category:", error);
    res.status(500).json({ error: "Failed to update fee category" });
  }
};

// DELETE /api/fee-categories/:id - Delete fee category
const deleteFeeCategory = async (req, res) => {
  try {
    const { id } = req.params;

    // Check if category is used in any bill templates
    const [usageCheck] = await pool.query(
      "SELECT id FROM bill_templates WHERE fee_category_id = ? LIMIT 1",
      [id],
    );

    if (usageCheck.length > 0) {
      return res.status(400).json({
        error:
          "Cannot delete fee category. It is being used in bill templates.",
      });
    }

    const [result] = await pool.query(
      "DELETE FROM fee_categories WHERE id = ?",
      [id],
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ error: "Fee category not found" });
    }

    res.json({ message: "Fee category deleted successfully" });
  } catch (error) {
    console.error("Error deleting fee category:", error);
    res.status(500).json({ error: "Failed to delete fee category" });
  }
};

//bill template controllers
// GET /api/bill-templates - Get all bill templates with related data
const getBillTemplates = async (req, res) => {
  try {
    const [templates] = await pool.query(`
      SELECT 
        bt.*,
        c.class_name,
        ay.year_label as academic_year,
        t.term_name,
        fc.category_name
      FROM bill_templates bt
      LEFT JOIN classes c ON bt.class_id = c.id
      LEFT JOIN academic_years ay ON bt.academic_year_id = ay.id
      LEFT JOIN terms t ON bt.term_id = t.id
      LEFT JOIN fee_categories fc ON bt.fee_category_id = fc.id
      ORDER BY bt.created_at DESC
    `);
    res.json(templates);
  } catch (error) {
    console.error("Error fetching bill templates:", error);
    res.status(500).json({ error: "Failed to fetch bill templates" });
  }
};

// POST /api/bill-templates - Create new bill template
const createBillTemplate = async (req, res) => {
  try {
    const {
      class_id,
      academic_year_id,
      term_id,
      fee_category_id,
      amount,
      due_date,
      is_compulsory,
      description,
    } = req.body;

    // Check if template already exists for same class/year/term/category
    const [existing] = await pool.query(
      `SELECT id FROM bill_templates 
       WHERE class_id = ? AND academic_year_id = ? AND term_id = ? AND fee_category_id = ?`,
      [class_id, academic_year_id, term_id, fee_category_id],
    );

    if (existing.length > 0) {
      return res.status(400).json({
        error:
          "A bill template already exists for this class, academic year, term, and fee category combination",
      });
    }

    const [result] = await pool.query(
      `INSERT INTO bill_templates 
       (class_id, academic_year_id, term_id, fee_category_id, amount, due_date, is_compulsory, description) 
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        class_id,
        academic_year_id,
        term_id,
        fee_category_id,
        amount,
        due_date,
        is_compulsory,
        description,
      ],
    );
    clearRelevantCaches("UPDATE_BILL_TEMPLATE");

    const [newTemplate] = await pool.query(
      `SELECT bt.*, c.class_name, ay.year_label as academic_year, t.term_name, fc.category_name
       FROM bill_templates bt
       LEFT JOIN classes c ON bt.class_id = c.id
       LEFT JOIN academic_years ay ON bt.academic_year_id = ay.id
       LEFT JOIN terms t ON bt.term_id = t.id
       LEFT JOIN fee_categories fc ON bt.fee_category_id = fc.id
       WHERE bt.id = ?`,
      [result.insertId],
    );

    res.status(201).json(newTemplate[0]);
  } catch (error) {
    console.error("Error creating bill template:", error);
    res.status(500).json({ error: "Failed to create bill template" });
  }
};

// PUT /api/bill-templates/:id - Update bill template
const updateBillTemplate = async (req, res) => {
  try {
    const {
      class_id,
      academic_year_id,
      term_id,
      fee_category_id,
      amount,
      due_date,
      is_compulsory,
      description,
    } = req.body;

    // Check if template exists
    const [existing] = await pool.query(
      "SELECT id FROM bill_templates WHERE id = ?",
      [req.params.id],
    );

    if (existing.length === 0) {
      return res.status(404).json({ error: "Bill template not found" });
    }

    // Check for duplicate (excluding current template)
    const [duplicate] = await pool.query(
      `SELECT id FROM bill_templates 
       WHERE class_id = ? AND academic_year_id = ? AND term_id = ? AND fee_category_id = ? AND id != ?`,
      [class_id, academic_year_id, term_id, fee_category_id, req.params.id],
    );

    if (duplicate.length > 0) {
      return res.status(400).json({
        error:
          "Another bill template already exists for this class, academic year, term, and fee category combination",
      });
    }

    await pool.query(
      `UPDATE bill_templates SET 
       class_id = ?, academic_year_id = ?, term_id = ?, fee_category_id = ?, 
       amount = ?, due_date = ?, is_compulsory = ?, description = ?
       WHERE id = ?`,
      [
        class_id,
        academic_year_id,
        term_id,
        fee_category_id,
        amount,
        due_date,
        is_compulsory,
        description,
        req.params.id,
      ],
    );

    const [updatedTemplate] = await pool.query(
      `SELECT bt.*, c.class_name, ay.year_label as academic_year, t.term_name, fc.category_name
       FROM bill_templates bt
       LEFT JOIN classes c ON bt.class_id = c.id
       LEFT JOIN academic_years ay ON bt.academic_year_id = ay.id
       LEFT JOIN terms t ON bt.term_id = t.id
       LEFT JOIN fee_categories fc ON bt.fee_category_id = fc.id
       WHERE bt.id = ?`,
      [req.params.id],
    );

    res.json(updatedTemplate[0]);
  } catch (error) {
    console.error("Error updating bill template:", error);
    res.status(500).json({ error: "Failed to update bill template" });
  }
};

// DELETE /api/bill-templates/:id - Delete bill template
const deleteBillTemplate = async (req, res) => {
  try {
    const { id } = req.params;

    // Check if template exists
    const [existing] = await pool.query(
      "SELECT id FROM bill_templates WHERE id = ?",
      [id],
    );

    if (existing.length === 0) {
      return res.status(404).json({ error: "Bill template not found" });
    }

    // Check if template has generated any student bills
    const [billsCheck] = await pool.query(
      "SELECT id FROM bills WHERE bill_template_id = ? LIMIT 1",
      [id],
    );

    if (billsCheck.length > 0) {
      return res.status(400).json({
        error:
          "Cannot delete bill template. It has already generated student bills.",
      });
    }

    await pool.query("DELETE FROM bill_templates WHERE id = ?", [id]);

    res.json({ message: "Bill template deleted successfully" });
  } catch (error) {
    console.error("Error deleting bill template:", error);
    res.status(500).json({ error: "Failed to delete bill template" });
  }
};

// Helper function to calculate student payment status
const calculateStudentPaymentStatus = (finalizedBill, bills) => {
  if (!finalizedBill) {
    return "Pending"; // No finalized bill means pending
  }

  const remainingBalance =
    parseFloat(finalizedBill.remaining_balance) ||
    parseFloat(finalizedBill.total_amount);
  const paidAmount = parseFloat(finalizedBill.paid_amount) || 0;
  const totalAmount = parseFloat(finalizedBill.total_amount);

  if (remainingBalance <= 0) {
    return "Paid";
  } else if (paidAmount > 0 && paidAmount < totalAmount) {
    return "Partially Paid";
  } else {
    return "Pending";
  }
};

//student bill controllers
// GET /api/student-bills - Get student bills with filters AND PAGINATION
const getStudentBills = async (req, res) => {
  try {
    const {
      class_id,
      academic_year_id,
      term_id,
      student_id,
      status,
      active_only,
      page = 1,
      limit = 20,
    } = req.query;

    let whereConditions = ["1=1"];
    let queryParams = [];

    if (active_only === "true") {
      whereConditions.push("(s.is_active IS NULL OR s.is_active = TRUE)");
    }

    if (class_id) {
      whereConditions.push(`
        b.student_id IN (
          SELECT student_id FROM class_assignments 
          WHERE class_id = ? AND academic_year_id = COALESCE(?, ca.academic_year_id)
        )
      `);
      queryParams.push(class_id);
      if (academic_year_id) {
        queryParams.push(academic_year_id);
      }
    }

    if (academic_year_id) {
      whereConditions.push("bt.academic_year_id = ?");
      queryParams.push(academic_year_id);
    }

    if (term_id) {
      whereConditions.push("bt.term_id = ?");
      queryParams.push(term_id);
    }

    if (student_id) {
      whereConditions.push("b.student_id = ?");
      queryParams.push(student_id);
    }

    // Calculate offset for pagination
    const pageNum = parseInt(page);
    const limitNum = parseInt(limit);
    const offset = (pageNum - 1) * limitNum;

    // FIRST: Get total count for pagination
    const [countResult] = await pool.query(
      `
      SELECT COUNT(DISTINCT s.id) as total
      FROM bills b
      LEFT JOIN bill_templates bt ON b.bill_template_id = bt.id
      LEFT JOIN students s ON b.student_id = s.id
      LEFT JOIN class_assignments ca ON s.id = ca.student_id AND bt.academic_year_id = ca.academic_year_id
      LEFT JOIN classes c ON ca.class_id = c.id
      LEFT JOIN academic_years ay ON bt.academic_year_id = ay.id
      LEFT JOIN terms t ON bt.term_id = t.id
      LEFT JOIN fee_categories fc ON bt.fee_category_id = fc.id
      LEFT JOIN student_term_bills stb ON (
        s.id = stb.student_id AND 
        bt.academic_year_id = stb.academic_year_id AND 
        bt.term_id = stb.term_id AND
        stb.is_finalized = TRUE
      )
      WHERE ${whereConditions.join(" AND ")}
      `,
      queryParams,
    );

    const total = countResult[0].total;
    const totalPages = Math.ceil(total / limitNum);

    // SECOND: Get paginated student bills
    const [bills] = await pool.query(
      `
      SELECT 
        b.*,
        bt.description,
        bt.is_compulsory,
        bt.academic_year_id,
        bt.term_id,
        s.first_name,
        s.last_name,
        s.admission_number,
        s.is_active,
        c.class_name,
        c.id as class_id,
        ay.year_label as academic_year,
        t.term_name,
        fc.category_name,
        stb.id as finalized_bill_id,
        stb.total_amount as finalized_total,
        stb.paid_amount as finalized_paid,
        stb.remaining_balance as finalized_balance,
        stb.is_fully_paid as finalized_fully_paid,
        stb.selected_bills as finalized_selected_bills
      FROM bills b
      LEFT JOIN bill_templates bt ON b.bill_template_id = bt.id
      LEFT JOIN students s ON b.student_id = s.id
      LEFT JOIN class_assignments ca ON s.id = ca.student_id AND bt.academic_year_id = ca.academic_year_id
      LEFT JOIN classes c ON ca.class_id = c.id
      LEFT JOIN academic_years ay ON bt.academic_year_id = ay.id
      LEFT JOIN terms t ON bt.term_id = t.id
      LEFT JOIN fee_categories fc ON bt.fee_category_id = fc.id
      LEFT JOIN student_term_bills stb ON (
        s.id = stb.student_id AND 
        bt.academic_year_id = stb.academic_year_id AND 
        bt.term_id = stb.term_id AND
        stb.is_finalized = TRUE
      )
      WHERE ${whereConditions.join(" AND ")}
      GROUP BY s.id, b.id
      ORDER BY s.first_name, s.last_name, b.due_date ASC
      LIMIT ? OFFSET ?
      `,
      [...queryParams, limitNum, offset],
    );

    // Process bills to apply edited amounts
    const processedBills = bills.map((bill) => {
      let finalBill = { ...bill };

      if (
        bill.finalized_selected_bills &&
        typeof bill.finalized_selected_bills === "string"
      ) {
        try {
          const selectedBillsData = JSON.parse(bill.finalized_selected_bills);
          const editedAmounts = selectedBillsData.edited_amounts || {};

          if (editedAmounts[bill.id]) {
            finalBill.finalized_amount = editedAmounts[bill.id];
            finalBill.amount = editedAmounts[bill.id];
            finalBill.has_custom_amount = true;
            finalBill.original_amount = bill.amount;
          }
        } catch (e) {
          console.error("Error parsing selected_bills:", e);
        }
      }

      return finalBill;
    });

    res.json({
      bills: processedBills,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total,
        totalPages,
        hasNextPage: pageNum < totalPages,
        hasPrevPage: pageNum > 1,
      },
    });
  } catch (error) {
    console.error("Error fetching student bills:", error);
    res.status(500).json({ error: "Failed to fetch student bills" });
  }
};

// GET /api/students-with-previous-balances - Improved version
const getStudentsWithPreviousBalances = async (req, res) => {
  try {
    const { academic_year_id, term_id, class_id } = req.query;

    // First, get the latest balance for each student
    const query = `
      WITH LatestBalance AS (
        SELECT 
          stb.student_id,
          stb.remaining_balance,
          stb.total_amount,
          stb.paid_amount,
          ay.year_label,
          t.term_name,
          stb.academic_year_id,
          stb.term_id,
          ROW_NUMBER() OVER (
            PARTITION BY stb.student_id 
            ORDER BY stb.academic_year_id DESC, stb.term_id DESC
          ) as rn
        FROM student_term_bills stb
        JOIN academic_years ay ON stb.academic_year_id = ay.id
        JOIN terms t ON stb.term_id = t.id
        WHERE stb.is_finalized = TRUE
          AND stb.remaining_balance > 0
          AND (stb.academic_year_id < ? OR (stb.academic_year_id = ? AND stb.term_id < ?))
      )
      SELECT 
        s.id,
        s.admission_number,
        s.first_name,
        s.last_name,
        lb.remaining_balance as outstanding_balance,
        CONCAT(lb.year_label, ' - Term ', lb.term_name) as previous_period,
        lb.academic_year_id as prev_academic_year_id,
        lb.term_id as prev_term_id,
        (SELECT COUNT(*) FROM student_arrears sa 
         WHERE sa.student_id = s.id 
           AND sa.academic_year_id = ? 
           AND sa.term_id = ? 
           AND sa.is_carried_forward = TRUE) as has_existing_carry
      FROM LatestBalance lb
      JOIN students s ON lb.student_id = s.id
      WHERE lb.rn = 1  -- Get only the latest balance per student
        AND (s.is_active IS NULL OR s.is_active = TRUE)
    `;

    const params = [
      academic_year_id,
      academic_year_id,
      term_id,
      academic_year_id,
      term_id,
    ];

    // Add class filter if provided
    let finalQuery = query;
    if (class_id) {
      finalQuery += ` AND s.id IN (
        SELECT student_id FROM class_assignments 
        WHERE class_id = ? AND academic_year_id = ?
      )`;
      params.push(class_id, academic_year_id);
    }

    finalQuery += ` ORDER BY lb.academic_year_id DESC, lb.term_id DESC`;

    const [students] = await pool.query(finalQuery, params);

    // Filter out students who already have carried balances
    const studentsToProcess = students.filter(
      (s) => s.has_existing_carry === 0,
    );

    res.json(studentsToProcess);
  } catch (error) {
    console.error("Error getting students with previous balances:", error);
    res.status(500).json({
      error: "Failed to get students with previous balances",
      details: error.message,
    });
  }
};

const generateBillsFromTemplates = async (req, res) => {
  const connection = await pool.getConnection();

  try {
    await connection.beginTransaction();

    const { class_id, academic_year_id, term_id, created_by = 1 } = req.body;

    // Get current term info
    const [currentTerm] = await connection.query(
      "SELECT term_name FROM terms WHERE id = ?",
      [term_id],
    );
    const termName = currentTerm[0]?.term_name || `Term ${term_id}`;

    const [currentYear] = await connection.query(
      "SELECT year_label FROM academic_years WHERE id = ?",
      [academic_year_id],
    );
    const currentYearLabel = currentYear[0]?.year_label;

    // Get count before deletion for reporting
    await connection.query("SELECT COUNT(*) as total FROM student_arrears");

    // Delete all arrears
    await connection.query("DELETE FROM student_arrears");

    await connection.beginTransaction();

    // Get count before deletion for reporting
    await connection.query(
      "SELECT COUNT(*) as total FROM student_overpayments",
    );

    // Delete all overpayments
    await connection.query("DELETE FROM student_overpayments");

    // FIXED QUERY: Only carry balances from immediate previous term
    const [studentsWithImmediateBalances] = await connection.query(
      `
      SELECT 
        s.id as student_id,
        s.admission_number,
        s.first_name,
        s.last_name,
        stb.remaining_balance as outstanding_balance,
        CONCAT(ay.year_label, ' - ', t.term_name) as previous_period,
        stb.academic_year_id as prev_academic_year_id,
        stb.term_id as prev_term_id,
        (SELECT COUNT(*) FROM student_arrears sa 
         WHERE sa.student_id = s.id 
           AND sa.academic_year_id = ? 
           AND sa.term_id = ?) as existing_arrears_count
      FROM student_term_bills stb
      INNER JOIN students s ON stb.student_id = s.id
      INNER JOIN academic_years ay ON stb.academic_year_id = ay.id
      INNER JOIN terms t ON stb.term_id = t.id
      WHERE stb.is_finalized = TRUE
        AND stb.remaining_balance > 0
        AND (
          -- Case 1: Previous term in same academic year (for Term 2 or 3)
          (
            stb.academic_year_id = ?  -- current academic year
            AND stb.term_id = (
              SELECT MAX(id) 
              FROM terms 
              WHERE academic_year_id = ? 
                AND id < ?  -- current term id
            )
          )
          -- REMOVED: Case 2 for previous year's last term
          -- Only include previous year's last term if we're in Term 1
          -- (We'll handle this with a separate check below)
        )
        AND s.id IN (
          SELECT student_id 
          FROM class_assignments 
          WHERE class_id = ? 
            AND academic_year_id = ?  -- current academic year
        )
        AND (s.is_active IS NULL OR s.is_active = TRUE)
      ORDER BY s.first_name, s.last_name
      `,
      [
        academic_year_id,
        term_id, // for existing_arrears_count
        academic_year_id, // Case 1: same academic year
        academic_year_id,
        term_id, // Case 1: subquery
        class_id,
        academic_year_id, // for class assignments
      ],
    );

    // CHECK IF WE'RE IN TERM 1 - If yes, also get previous year's last term balances
    let previousYearBalances = [];
    const [termInfo] = await connection.query(
      `SELECT 
        t.id,
        t.term_name,
        (SELECT MIN(id) FROM terms WHERE academic_year_id = ?) as first_term_id
      FROM terms t
      WHERE t.id = ?`,
      [academic_year_id, term_id],
    );

    const isFirstTerm =
      termInfo.length > 0 && termInfo[0].id === termInfo[0].first_term_id;

    if (isFirstTerm) {
      // Get previous year's last term balances for Term 1 only
      const [prevYearBalances] = await connection.query(
        `
        SELECT 
          s.id as student_id,
          s.admission_number,
          s.first_name,
          s.last_name,
          stb.remaining_balance as outstanding_balance,
          CONCAT(ay.year_label, ' - ', t.term_name) as previous_period,
          stb.academic_year_id as prev_academic_year_id,
          stb.term_id as prev_term_id,
          (SELECT COUNT(*) FROM student_arrears sa 
           WHERE sa.student_id = s.id 
             AND sa.academic_year_id = ? 
             AND sa.term_id = ?) as existing_arrears_count
        FROM student_term_bills stb
        INNER JOIN students s ON stb.student_id = s.id
        INNER JOIN academic_years ay ON stb.academic_year_id = ay.id
        INNER JOIN terms t ON stb.term_id = t.id
        WHERE stb.is_finalized = TRUE
          AND stb.remaining_balance > 0
          -- Previous year's last term (only for Term 1)
          AND stb.academic_year_id = (
            SELECT MAX(id) 
            FROM academic_years 
            WHERE end_date < (
              SELECT start_date 
              FROM academic_years 
              WHERE id = ?  -- current academic year
            )
          )
          AND stb.term_id = (
            SELECT MAX(id) 
            FROM terms 
            WHERE academic_year_id = stb.academic_year_id
          )
          AND s.id IN (
            SELECT student_id 
            FROM class_assignments 
            WHERE class_id = ? 
              AND academic_year_id = ?  -- current academic year
          )
          AND (s.is_active IS NULL OR s.is_active = TRUE)
        ORDER BY s.first_name, s.last_name
        `,
        [
          academic_year_id,
          term_id, // for existing_arrears_count
          academic_year_id, // for finding previous year
          class_id,
          academic_year_id, // for class assignments
        ],
      );

      previousYearBalances = prevYearBalances;
    }

    // Combine both sets of balances
    const allBalances = [
      ...studentsWithImmediateBalances,
      ...previousYearBalances,
    ];

    // Remove duplicates (if a student appears in both lists somehow)
    const uniqueBalances = allBalances.filter(
      (balance, index, self) =>
        index === self.findIndex((b) => b.student_id === balance.student_id),
    );

    console.log("Found students with immediate previous term balances:", {
      count: uniqueBalances.length,
      current_term: `${currentYearLabel} - ${termName}`,
      is_first_term: isFirstTerm,
      students: uniqueBalances.map((s) => ({
        name: `${s.first_name} ${s.last_name}`,
        balance: s.outstanding_balance,
        previous_period: s.previous_period,
        existing_arrears: s.existing_arrears_count,
      })),
    });

    // Get all bill templates
    const [templates] = await connection.query(
      `SELECT * FROM bill_templates 
       WHERE class_id = ? AND academic_year_id = ? AND term_id = ?`,
      [class_id, academic_year_id, term_id],
    );

    if (templates.length === 0) {
      await connection.rollback();
      return res.status(400).json({ error: "No bill templates found" });
    }

    // Get all students in the class (for generating bills even if no previous balance)
    const [allStudents] = await connection.query(
      `SELECT 
         ca.student_id,
         s.first_name,
         s.last_name,
         s.admission_number
       FROM class_assignments ca
       JOIN students s ON ca.student_id = s.id
       WHERE ca.class_id = ? AND ca.academic_year_id = ?
         AND (s.is_active IS NULL OR s.is_active = TRUE)
       ORDER BY s.first_name, s.last_name`,
      [class_id, academic_year_id],
    );

    if (allStudents.length === 0) {
      await connection.rollback();
      return res.status(400).json({ error: "No students found" });
    }

    let generatedBillsCount = 0;
    let carriedBalancesCount = 0;
    const carriedBalances = [];
    const errors = [];

    // Create a map of students with immediate balances for quick lookup
    const immediateBalanceMap = new Map();
    uniqueBalances.forEach((student) => {
      immediateBalanceMap.set(student.student_id, student);
    });

    // Process all students in the class
    for (const student of allStudents) {
      try {
        // Check if this student has an immediate previous term balance
        const balanceData = immediateBalanceMap.get(student.student_id);

        // ==================== CRITICAL FIX ====================
        // STEP 0: CLEAR EXISTING ARREARS for this student/term FIRST
        if (balanceData && balanceData.existing_arrears_count > 0) {
          console.log(
            `Clearing existing arrears for student ${student.student_id} for term ${academic_year_id}/${term_id}`,
          );
          await connection.query(
            `DELETE FROM student_arrears 
             WHERE student_id = ? 
               AND academic_year_id = ? 
               AND term_id = ?`,
            [student.student_id, academic_year_id, term_id],
          );
        }

        // STEP 1: Carry forward balance if exists
        if (balanceData && balanceData.outstanding_balance > 0) {
          const previousBalanceAmount = parseFloat(
            balanceData.outstanding_balance,
          );

          // Create arrears entry
          await connection.query(
            `INSERT INTO student_arrears 
             (student_id, description, amount, original_amount, 
              academic_year_id, term_id, is_carried_forward,
              carried_from_academic_year_id, carried_from_term_id,
              created_by) 
             VALUES (?, ?, ?, ?, ?, ?, TRUE, ?, ?, ?)`,
            [
              student.student_id,
              `Previous balance from ${
                balanceData.previous_period
              } - Ghc ${previousBalanceAmount.toFixed(2)}`,
              previousBalanceAmount,
              previousBalanceAmount,
              academic_year_id,
              term_id,
              balanceData.prev_academic_year_id,
              balanceData.prev_term_id,
              created_by,
            ],
          );

          carriedBalancesCount++;
          carriedBalances.push({
            student_id: student.student_id,
            student_name: `${student.first_name} ${student.last_name}`,
            admission_number: student.admission_number,
            previous_balance: previousBalanceAmount,
            previous_period: balanceData.previous_period,
            current_term: `${currentYearLabel} - ${termName}`,
            cleared_existing_arrears: balanceData.existing_arrears_count > 0,
          });
        }

        // STEP 2: Generate bills from templates for ALL students
        for (const template of templates) {
          // Check if bill already exists
          const [existingBill] = await connection.query(
            `SELECT id FROM bills 
             WHERE student_id = ? AND bill_template_id = ?`,
            [student.student_id, template.id],
          );

          if (existingBill.length === 0) {
            // Create new bill
            await connection.query(
              `INSERT INTO bills (bill_template_id, student_id, amount, due_date, is_compulsory, description, status)
               VALUES (?, ?, ?, ?, ?, ?, 'Pending')`,
              [
                template.id,
                student.student_id,
                template.amount,
                template.due_date,
                template.is_compulsory,
                template.description ||
                  `${template.category_name} - ${termName}`,
              ],
            );
            generatedBillsCount++;
          }
        }
      } catch (error) {
        errors.push({
          student_id: student.student_id,
          student_name: `${student.first_name} ${student.last_name}`,
          error: error.message,
        });
      }
    }

    await connection.commit();

    // Success response
    res.json({
      success: true,
      message: `Successfully generated ${generatedBillsCount} student bills`,
      generated: generatedBillsCount,
      carriedBalances: carriedBalancesCount,
      carriedBalances: carriedBalances,
      total_students: allStudents.length,
      students_with_immediate_balances: uniqueBalances.length,
      is_first_term: isFirstTerm,
      errors: errors,
      summary: {
        current_term: `${currentYearLabel} - ${termName}`,
        students_in_class: allStudents.length,
        students_with_immediate_balances: uniqueBalances.length,
        balances_carried: carriedBalancesCount,
        existing_arrears_cleared: carriedBalances.filter(
          (b) => b.cleared_existing_arrears,
        ).length,
      },
    });
  } catch (error) {
    await connection.rollback();
    console.error("Error generating bills:", error);
    res.status(500).json({
      error: "Failed to generate bills",
      details: error.message,
      stack: process.env.NODE_ENV === "development" ? error.stack : undefined,
    });
  } finally {
    connection.release();
  }
};

const saveStudentTermBill = async (req, res) => {
  const connection = await pool.getConnection();

  try {
    await connection.beginTransaction();

    const {
      student_id,
      academic_year_id,
      term_id,
      total_amount,
      compulsory_amount,
      optional_amount,
      selected_bills,
      edited_amounts,
      created_by,
      arrears_bill_id,
      apply_overpayments_to_bill_id,
    } = req.body;

    // Fetch arrears and overpayments
    const [arrears] = await connection.query(
      "SELECT SUM(amount) as total_arrears FROM student_arrears WHERE student_id = ? AND academic_year_id = ?",
      [student_id, academic_year_id],
    );

    const [overpayments] = await connection.query(
      "SELECT SUM(amount) as total_overpayments FROM student_overpayments WHERE student_id = ? AND academic_year_id = ? AND status = 'Active'",
      [student_id, academic_year_id],
    );

    const arrearsTotal = parseFloat(arrears[0]?.total_arrears || 0);
    const overpaymentsTotal = parseFloat(
      overpayments[0]?.total_overpayments || 0,
    );

    // Get the bill to adjust (default to first compulsory if not specified)
    let targetBillId = arrears_bill_id;
    if (!targetBillId && selected_bills.length > 0) {
      // Find first compulsory bill
      const [firstCompulsory] = await connection.query(
        `SELECT b.id FROM bills b 
         JOIN bill_templates bt ON b.bill_template_id = bt.id
         WHERE b.id IN (?) AND bt.is_compulsory = TRUE
         LIMIT 1`,
        [selected_bills],
      );
      targetBillId = firstCompulsory[0]?.id;
    }

    // Get overpayment target bill (can be same or different)
    let overpaymentBillId = apply_overpayments_to_bill_id || targetBillId;

    // Prepare final edited_amounts with arrears/overpayments baked in
    const finalEditedAmounts = { ...(edited_amounts || {}) };

    // Apply arrears to target bill
    if (targetBillId && arrearsTotal > 0) {
      const [targetBill] = await connection.query(
        `SELECT b.amount, bt.is_compulsory 
         FROM bills b
         JOIN bill_templates bt ON b.bill_template_id = bt.id
         WHERE b.id = ?`,
        [targetBillId],
      );

      if (targetBill.length > 0) {
        const baseAmount = parseFloat(targetBill[0].amount);
        const currentEditedAmount =
          finalEditedAmounts[targetBillId] || baseAmount;
        finalEditedAmounts[targetBillId] = currentEditedAmount + arrearsTotal;
      }
    }

    // Apply overpayments to target bill
    if (overpaymentBillId && overpaymentsTotal > 0) {
      const [targetBill] = await connection.query(
        `SELECT b.amount FROM bills b WHERE b.id = ?`,
        [overpaymentBillId],
      );

      if (targetBill.length > 0) {
        const baseAmount =
          finalEditedAmounts[overpaymentBillId] ||
          parseFloat(targetBill[0].amount);

        // Don't let bill go negative
        finalEditedAmounts[overpaymentBillId] = Math.max(
          0,
          baseAmount - overpaymentsTotal,
        );
      }
    }
    // If there's leftover overpayment after zeroing this bill,
    // we could apply to other bills, but for now we'll just cap at 0

    // Prepare selected_bills data WITH metadata
    const selectedBillsData = {
      bill_ids: selected_bills,
      edited_amounts: finalEditedAmounts,
      arrears_applied: {
        bill_id: targetBillId,
        amount: arrearsTotal,
      },
      overpayments_applied: {
        bill_id: overpaymentBillId,
        amount: overpaymentsTotal,
      },
    };

    // Check if term bill already exists
    const [existing] = await connection.query(
      `SELECT id FROM student_term_bills 
           WHERE student_id = ? AND academic_year_id = ? AND term_id = ?`,
      [student_id, academic_year_id, term_id],
    );

    // CRITICAL: Initialize individual bill payment status
    for (const billId of selected_bills) {
      await connection.query(
        `UPDATE bills SET 
              paid_amount = 0,
              remaining_amount = amount,
              payment_status = 'Pending'
             WHERE id = ?`,
        [billId],
      );
    }

    if (existing.length > 0) {
      // Update existing term bill
      await connection.query(
        `UPDATE student_term_bills SET 
             total_amount = ?, compulsory_amount = ?, optional_amount = ?, 
             selected_bills = ?, is_finalized = TRUE, 
             paid_amount = 0,  
             remaining_balance = ?,  
             is_fully_paid = FALSE,  
             last_payment_date = NULL,  
             updated_at = CURRENT_TIMESTAMP
             WHERE student_id = ? AND academic_year_id = ? AND term_id = ?`,
        [
          total_amount,
          compulsory_amount,
          optional_amount,
          JSON.stringify(selectedBillsData),
          total_amount,
          student_id,
          academic_year_id,
          term_id,
        ],
      );
    } else {
      // Create new term bill
      await connection.query(
        `INSERT INTO student_term_bills 
             (student_id, academic_year_id, term_id, total_amount, compulsory_amount, 
              optional_amount, selected_bills, is_finalized, 
              paid_amount, remaining_balance, is_fully_paid, created_by) 
             VALUES (?, ?, ?, ?, ?, ?, ?, TRUE, 0, ?, FALSE, ?)`,
        [
          student_id,
          academic_year_id,
          term_id,
          total_amount,
          compulsory_amount,
          optional_amount,
          JSON.stringify(selectedBillsData),
          total_amount, // remaining_balance starts as total_amount
          created_by,
        ],
      );
    }

    await connection.commit();
    clearRelevantCaches("UPDATE_TERM_BILL", {
      studentId: student_id,
      academicYearId: academic_year_id,
      termId: term_id,
    });
    res.json({
      message: "Student term bill finalized successfully",
      finalized: true,
      included_arrears: arrearsTotal,
      included_overpayments: overpaymentsTotal,
      total_amount: total_amount,
      remaining_balance: total_amount,
      bills_initialized: selected_bills.length, // Confirm bills were initialized
    });
  } catch (error) {
    await connection.rollback();
    console.error("Error saving student term bill:", error);
    res.status(500).json({ error: "Failed to save student term bill" });
  } finally {
    connection.release();
  }
};

// GET /api/student-term-bill/:studentId - Get finalized term bill for a student
const getStudentTermBill = async (req, res) => {
  try {
    const { studentId } = req.params;
    const { academic_year_id, term_id } = req.query;

    if (!academic_year_id || !term_id) {
      return res
        .status(400)
        .json({ error: "Academic year and term are required" });
    }

    const [termBills] = await pool.query(
      `SELECT stb.*, ay.year_label, t.term_name
       FROM student_term_bills stb
       LEFT JOIN academic_years ay ON stb.academic_year_id = ay.id
       LEFT JOIN terms t ON stb.term_id = t.id
       WHERE stb.student_id = ? 
         AND stb.academic_year_id = ? 
         AND stb.term_id = ?
         AND stb.is_finalized = TRUE`,
      [studentId, academic_year_id, term_id],
    );

    if (termBills.length === 0) {
      return res.status(404).json({ error: "No finalized term bill found" });
    }

    // Parse selected_bills if it's a string, otherwise return as-is
    const termBill = termBills[0];
    if (
      termBill.selected_bills &&
      typeof termBill.selected_bills === "string"
    ) {
      termBill.selected_bills = JSON.parse(termBill.selected_bills);
    }
    res.json(termBill);
  } catch (error) {
    console.error("Error fetching student term bill:", error);
    res.status(500).json({ error: "Failed to fetch student term bill" });
  }
};

//check payments for student bills in a specific term
const checkStudentPayments = async (req, res) => {
  try {
    const { studentId } = req.params;
    const { academic_year_id, term_id } = req.query;

    console.log("Checking payments for:", {
      studentId,
      academic_year_id,
      term_id,
    });

    // Check if any payments exist for this student's bills in the SPECIFIC term
    const [payments] = await pool.query(
      `SELECT COUNT(p.id) as payment_count
       FROM payments p
       INNER JOIN receipts r ON p.id = r.payment_id
       WHERE p.student_id = ? 
         AND r.academic_year_id = ? 
         AND r.term_id = ?`,
      [studentId, academic_year_id, term_id],
    );

    console.log("Payment count result:", payments[0]);

    res.json({
      hasPayments: payments[0].payment_count > 0,
      paymentCount: payments[0].payment_count,
      academic_year_id: academic_year_id,
      term_id: term_id,
    });
  } catch (error) {
    console.error("Error checking student payments:", error);
    res.status(500).json({ error: "Failed to check student payments" });
  }
};

// get available bills for students
const getAvailableBillsForStudent = async (req, res) => {
  try {
    const { student_id, academic_year_id, term_id } = req.query;

    if (!student_id || !academic_year_id || !term_id) {
      return res.status(400).json({
        error: "student_id, academic_year_id, and term_id are required",
      });
    }

    // Get ALL bills for this student/year/term
    const [bills] = await pool.query(
      `
      SELECT 
        b.*,
        bt.description,
        bt.is_compulsory,
        bt.academic_year_id,
        bt.term_id,
        fc.category_name
      FROM bills b
      LEFT JOIN bill_templates bt ON b.bill_template_id = bt.id
      LEFT JOIN fee_categories fc ON bt.fee_category_id = fc.id
      WHERE b.student_id = ? 
        AND bt.academic_year_id = ? 
        AND bt.term_id = ?
      ORDER BY bt.is_compulsory DESC, fc.category_name
      `,
      [student_id, academic_year_id, term_id],
    );

    // Also get the finalized term bill to see which bills are already included
    const [termBill] = await pool.query(
      `SELECT selected_bills FROM student_term_bills 
       WHERE student_id = ? AND academic_year_id = ? AND term_id = ? AND is_finalized = TRUE`,
      [student_id, academic_year_id, term_id],
    );

    let alreadyIncludedBillIds = [];
    if (termBill.length > 0) {
      const selectedBillsData = termBill[0].selected_bills;
      if (selectedBillsData && typeof selectedBillsData === "string") {
        try {
          const parsed = JSON.parse(selectedBillsData);
          alreadyIncludedBillIds = parsed.bill_ids || [];
        } catch (e) {
          console.error("Error parsing selected_bills:", e);
        }
      }
    }

    // Separate bills
    const allBills = bills || [];
    const availableOptionalBills = allBills.filter(
      (bill) =>
        !bill.is_compulsory && !alreadyIncludedBillIds.includes(bill.id),
    );

    res.json({
      all_bills: allBills,
      available_optional_bills: availableOptionalBills,
      already_included_ids: alreadyIncludedBillIds,
      total_bills: allBills.length,
      available_count: availableOptionalBills.length,
    });
  } catch (error) {
    console.error("Error fetching available bills:", error);
    res.status(500).json({ error: "Failed to fetch available bills" });
  }
};

// POST /api/student-term-bills/add-bills - Add bills to finalized term bill
const addBillsToFinalizedTerm = async (req, res) => {
  const connection = await pool.getConnection();

  try {
    await connection.beginTransaction();

    const {
      student_id,
      academic_year_id,
      term_id,
      new_bill_ids,
      edited_amounts,
      created_by,
    } = req.body;

    // 1. Get current finalized term bill WITH LOCK
    const [termBill] = await connection.query(
      `SELECT * FROM student_term_bills 
       WHERE student_id = ? AND academic_year_id = ? AND term_id = ? AND is_finalized = TRUE
       FOR UPDATE`,
      [student_id, academic_year_id, term_id],
    );

    if (termBill.length === 0) {
      await connection.rollback();
      return res.status(404).json({ error: "No finalized term bill found" });
    }

    const currentTermBill = termBill[0];

    // 2. Parse existing selected bills
    let selectedBillsData = {};
    try {
      selectedBillsData =
        typeof currentTermBill.selected_bills === "string"
          ? JSON.parse(currentTermBill.selected_bills)
          : currentTermBill.selected_bills;
    } catch (error) {
      selectedBillsData = {};
    }

    // 3. Get existing arrays
    const existingBillIds = selectedBillsData.bill_ids || [];
    const existingEditedAmounts = selectedBillsData.edited_amounts || {};

    // 4. Check which bills are new
    const billsToAdd = new_bill_ids.filter(
      (billId) => !existingBillIds.includes(billId),
    );

    if (billsToAdd.length === 0) {
      await connection.rollback();
      return res.status(400).json({ error: "No new bills to add" });
    }

    // 5. CRITICAL FIX: Update the bill records' remaining_amount before adding to finalized bill
    const [newBills] = await connection.query(
      `SELECT id, amount, is_compulsory, description FROM bills WHERE id IN (?)`,
      [billsToAdd],
    );

    // For each new bill, update its remaining_amount to match the final amount
    for (const bill of newBills) {
      const editedAmount = edited_amounts && edited_amounts[bill.id];
      const finalAmount =
        editedAmount !== undefined
          ? parseFloat(editedAmount)
          : parseFloat(bill.amount);

      // Update the bill's payment status fields
      await connection.query(
        `UPDATE bills SET 
          remaining_amount = ?,
          paid_amount = 0,
          payment_status = 'Pending'
         WHERE id = ?`,
        [finalAmount, bill.id],
      );
    }

    // 6. Calculate new totals
    let compulsoryIncrease = 0;
    let optionalIncrease = 0;

    newBills.forEach((bill) => {
      const editedAmount = edited_amounts && edited_amounts[bill.id];
      const amount =
        editedAmount !== undefined
          ? parseFloat(editedAmount)
          : parseFloat(bill.amount);

      if (bill.is_compulsory) {
        compulsoryIncrease += amount;
      } else {
        optionalIncrease += amount;
      }
    });

    // Get current totals
    const currentTotal = parseFloat(currentTermBill.total_amount) || 0;
    const currentCompulsory =
      parseFloat(currentTermBill.compulsory_amount) || 0;
    const currentOptional = parseFloat(currentTermBill.optional_amount) || 0;

    // Calculate NEW totals
    const newCompulsoryTotal = currentCompulsory + compulsoryIncrease;
    const newOptionalTotal = currentOptional + optionalIncrease;
    const newTotalAmount = currentTotal + compulsoryIncrease + optionalIncrease;

    // 7. Create updated selected bills array
    const updatedBillIds = [...existingBillIds, ...billsToAdd];

    // Add new bills with their final amounts
    const updatedEditedAmounts = { ...existingEditedAmounts };
    newBills.forEach((bill) => {
      if (updatedEditedAmounts[bill.id] === undefined) {
        const editedAmount = edited_amounts && edited_amounts[bill.id];
        updatedEditedAmounts[bill.id] =
          editedAmount !== undefined
            ? parseFloat(editedAmount)
            : parseFloat(bill.amount);
      }
    });

    // 8. Update the term bill
    const newRemainingBalance = Math.max(
      parseFloat(currentTermBill.remaining_balance) +
        compulsoryIncrease +
        optionalIncrease,
      0,
    );

    const updatedSelectedBillsData = {
      bill_ids: updatedBillIds,
      edited_amounts: updatedEditedAmounts,
      arrears_included: selectedBillsData.arrears_included || 0,
      overpayments_included: selectedBillsData.overpayments_included || 0,
      net_arrears_effect: selectedBillsData.net_arrears_effect || 0,
      added_bills: billsToAdd,
      added_at: new Date().toISOString(),
      added_by: created_by,
      new_bill_edits: edited_amounts
        ? Object.keys(edited_amounts).filter((id) =>
            billsToAdd.includes(parseInt(id)),
          )
        : [],
    };

    await connection.query(
      `UPDATE student_term_bills SET 
        total_amount = ?, 
        compulsory_amount = ?, 
        optional_amount = ?, 
        selected_bills = ?,
        remaining_balance = ?,
        is_fully_paid = ?,
        updated_at = CURRENT_TIMESTAMP
       WHERE id = ?`,
      [
        newTotalAmount,
        newCompulsoryTotal,
        newOptionalTotal,
        JSON.stringify(updatedSelectedBillsData),
        newRemainingBalance,
        newRemainingBalance <= 0,
        currentTermBill.id,
      ],
    );

    await connection.commit();
    // ADD THIS:
    clearRelevantCaches("UPDATE_TERM_BILL", {
      studentId: student_id,
      academicYearId: academic_year_id,
      termId: term_id,
    });

    // 9. Verify the bills were updated correctly
    for (const billId of billsToAdd) {
      const [verifyBill] = await connection.query(
        `SELECT id, amount, paid_amount, remaining_amount, payment_status FROM bills WHERE id = ?`,
        [billId],
      );
    }

    // 10. Return success
    res.json({
      success: true,
      message: `Added ${billsToAdd.length} bill(s) to the finalized term bill`,
      added_bills_count: billsToAdd.length,
      amount_added: (compulsoryIncrease + optionalIncrease).toFixed(2),
      previous_total: currentTotal.toFixed(2),
      new_total_amount: newTotalAmount.toFixed(2),
      updated_balance: newRemainingBalance.toFixed(2),
      bill_updates: {
        bills_updated: billsToAdd.length,
        remaining_amounts_set: true,
      },
    });
  } catch (error) {
    await connection.rollback();
    console.error("Error adding bills to finalized term:", error);
    res.status(500).json({
      error: "Failed to add bills to finalized term: " + error.message,
    });
  } finally {
    connection.release();
  }
};

// GET /api/student-arrears/:studentId - Get student arrears
const getStudentArrears = async (req, res) => {
  try {
    const { studentId } = req.params;
    const { academic_year_id, term_id } = req.query;

    let whereConditions = ["sa.student_id = ?"];
    let queryParams = [studentId];

    if (academic_year_id) {
      whereConditions.push("sa.academic_year_id = ?");
      queryParams.push(academic_year_id);
    }

    if (term_id) {
      whereConditions.push("sa.term_id = ?");
      queryParams.push(term_id);
    }

    const [arrears] = await pool.query(
      `SELECT sa.*, ay.year_label, t.term_name, u.username as created_by_name
       FROM student_arrears sa
       LEFT JOIN academic_years ay ON sa.academic_year_id = ay.id
       LEFT JOIN terms t ON sa.term_id = t.id
       LEFT JOIN users u ON sa.created_by = u.id
       WHERE ${whereConditions.join(" AND ")}
       ORDER BY sa.created_at DESC`,
      queryParams,
    );

    res.json(arrears);
  } catch (error) {
    console.error("Error fetching student arrears:", error);
    res.status(500).json({ error: "Failed to fetch student arrears" });
  }
};

// POST /api/student-arrears - Add student arrear
const addStudentArrear = async (req, res) => {
  try {
    const {
      student_id,
      description,
      amount,
      academic_year_id,
      term_id,
      created_by,
    } = req.body;

    // Validate required fields
    if (
      !student_id ||
      !description ||
      !amount ||
      !academic_year_id ||
      !term_id
    ) {
      return res.status(400).json({
        error:
          "Missing required fields. Please provide student_id, description, amount, academic_year_id, and term_id.",
      });
    }

    // Validate numeric fields
    if (isNaN(amount) || amount <= 0) {
      return res
        .status(400)
        .json({ error: "Amount must be a positive number" });
    }

    // Validate term_id is a valid integer
    if (isNaN(term_id) || !Number.isInteger(Number(term_id))) {
      return res.status(400).json({ error: "Invalid term_id" });
    }

    // Validate academic_year_id is a valid integer
    if (
      isNaN(academic_year_id) ||
      !Number.isInteger(Number(academic_year_id))
    ) {
      return res.status(400).json({ error: "Invalid academic_year_id" });
    }

    const [result] = await pool.query(
      `INSERT INTO student_arrears 
       (student_id, description, amount, original_amount, academic_year_id, term_id, created_by) 
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [
        student_id,
        description,
        amount,
        amount,
        academic_year_id,
        term_id,
        created_by,
      ],
    );

    const [newArrear] = await pool.query(
      `SELECT sa.*, ay.year_label, t.term_name
       FROM student_arrears sa
       LEFT JOIN academic_years ay ON sa.academic_year_id = ay.id
       LEFT JOIN terms t ON sa.term_id = t.id
       WHERE sa.id = ?`,
      [result.insertId],
    );

    res.status(201).json(newArrear[0]);
  } catch (error) {
    console.error("Error adding student arrear:", error);

    if (error.code === "ER_TRUNCATED_WRONG_VALUE_FOR_FIELD") {
      return res.status(400).json({
        error:
          "Invalid data format. Please check that all IDs are valid numbers.",
      });
    }

    res.status(500).json({ error: "Failed to add student arrear" });
  }
};

// DELETE /api/student-arrears/:id - Delete student arrear
const deleteStudentArrear = async (req, res) => {
  try {
    const { id } = req.params;

    const [result] = await pool.query(
      "DELETE FROM student_arrears WHERE id = ?",
      [id],
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ error: "Arrear not found" });
    }

    res.json({ message: "Arrear deleted successfully" });
  } catch (error) {
    console.error("Error deleting student arrear:", error);
    res.status(500).json({ error: "Failed to delete student arrear" });
  }
};

// // DELETE /api/student-arrears - Delete ALL arrears (use with caution!)
// const deleteAllArrears = async (req, res) => {
//   const connection = await pool.getConnection();

//   try {
//     await connection.beginTransaction();

//     // Get count before deletion for reporting
//     const [countResult] = await connection.query(
//       "SELECT COUNT(*) as total FROM student_arrears"
//     );
//     const totalCount = countResult[0].total;

//     // Delete all arrears
//     const [result] = await connection.query("DELETE FROM student_arrears");

//     await connection.commit();

//     res.json({
//       success: true,
//       message: `Successfully deleted all student arrears`,
//       deletedCount: totalCount,
//     });
//   } catch (error) {
//     await connection.rollback();
//     console.error("Error deleting all arrears:", error);
//     res.status(500).json({
//       error: "Failed to delete arrears",
//       details: error.message,
//     });
//   } finally {
//     connection.release();
//   }
// };

// GET /api/student-overpayments/:studentId - Get student overpayments

const getStudentOverpayments = async (req, res) => {
  try {
    const { studentId } = req.params;
    const { academic_year_id, term_id } = req.query;

    let whereConditions = ["so.student_id = ?"];
    let queryParams = [studentId];

    if (academic_year_id) {
      whereConditions.push("so.academic_year_id = ?");
      queryParams.push(academic_year_id);
    }

    if (term_id) {
      whereConditions.push("so.term_id = ?");
      queryParams.push(term_id);
    }

    const [overpayments] = await pool.query(
      `SELECT so.*, ay.year_label, t.term_name, u.username as created_by_name
       FROM student_overpayments so
       LEFT JOIN academic_years ay ON so.academic_year_id = ay.id
       LEFT JOIN terms t ON so.term_id = t.id
       LEFT JOIN users u ON so.created_by = u.id
       WHERE ${whereConditions.join(" AND ")}
       ORDER BY so.created_at DESC`,
      queryParams,
    );

    res.json(overpayments);
  } catch (error) {
    console.error("Error fetching student overpayments:", error);
    res.status(500).json({ error: "Failed to fetch student overpayments" });
  }
};

// POST /api/student-overpayments - Add student overpayment
const addStudentOverpayment = async (req, res) => {
  try {
    const {
      student_id,
      description,
      amount,
      academic_year_id,
      term_id,
      is_credit_note,
      can_refund,
      created_by,
    } = req.body;

    const [result] = await pool.query(
      `INSERT INTO student_overpayments 
       (student_id, description, amount, academic_year_id, term_id, is_credit_note, can_refund, created_by) 
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        student_id,
        description,
        amount,
        academic_year_id,
        term_id,
        is_credit_note,
        can_refund,
        created_by,
      ],
    );

    const [newOverpayment] = await pool.query(
      `SELECT so.*, ay.year_label, t.term_name
       FROM student_overpayments so
       LEFT JOIN academic_years ay ON so.academic_year_id = ay.id
       LEFT JOIN terms t ON so.term_id = t.id
       WHERE so.id = ?`,
      [result.insertId],
    );

    res.status(201).json(newOverpayment[0]);
  } catch (error) {
    console.error("Error adding student overpayment:", error);
    res.status(500).json({ error: "Failed to add student overpayment" });
  }
};

// DELETE /api/student-overpayments/:id - Delete student overpayment
const deleteStudentOverpayment = async (req, res) => {
  try {
    const { id } = req.params;

    const [result] = await pool.query(
      "DELETE FROM student_overpayments WHERE id = ?",
      [id],
    );
    if (result.affectedRows === 0) {
      return res.status(404).json({ error: "Overpayment not found" });
    }
    res.json({ message: "Overpayment deleted successfully" });
  } catch (error) {
    console.error("Error deleting student overpayment:", error);
    res.status(500).json({ error: "Failed to delete student overpayment" });
  }
};

// DELETE /api/student-overpayments - Delete ALL overpayments (use with caution!)
// const deleteAllOverpayments = async (req, res) => {
//   const connection = await pool.getConnection();

//   try {
//     await connection.beginTransaction();

//     // Get count before deletion for reporting
//     const [countResult] = await connection.query(
//       "SELECT COUNT(*) as total FROM student_overpayments"
//     );
//     const totalCount = countResult[0].total;

//     // Delete all overpayments
//     const [result] = await connection.query("DELETE FROM student_overpayments");

//     await connection.commit();

//     res.json({
//       success: true,
//       message: `Successfully deleted all student overpayments`,
//       deletedCount: totalCount,
//     });
//   } catch (error) {
//     await connection.rollback();
//     console.error("Error deleting all overpayments:", error);
//     res.status(500).json({
//       error: "Failed to delete overpayments",
//       details: error.message,
//     });
//   } finally {
//     connection.release();
//   }
// };

// FIXED generateClassBillsPDF function - Shows optional fees for non-finalized bills
const generateClassBillsPDF = async (req, res) => {
  try {
    const { class_id, academic_year_id, term_id } = req.params;

    // Get all students in the class
    const [students] = await pool.query(
      `
      SELECT 
        s.id as student_id,
        s.first_name,
        s.last_name, 
        s.admission_number,
        s.date_of_birth,
        s.gender,
        s.parent_name,
        s.parent_contact,
        c.class_name,
        ay.year_label as academic_year
      FROM students s
      INNER JOIN class_assignments ca ON s.id = ca.student_id
      LEFT JOIN classes c ON ca.class_id = c.id
      INNER JOIN academic_years ay ON ca.academic_year_id = ay.id
      WHERE ca.class_id = ? AND ca.academic_year_id = ? 
      AND (s.is_active IS NULL OR s.is_active = TRUE)
      ORDER BY s.first_name, s.last_name
      `,
      [class_id, academic_year_id],
    );

    if (students.length === 0) {
      return res.status(404).json({ error: "No students found in this class" });
    }

    // Get class info
    const [classInfo] = await pool.query(
      "SELECT class_name FROM classes WHERE id = ?",
      [class_id],
    );

    const className = classInfo[0]?.class_name || "class";

    // Get term info
    const [termInfo] = await pool.query(
      "SELECT term_name FROM terms WHERE id = ?",
      [term_id],
    );

    const termName = termInfo[0]?.term_name || `Term ${term_id}`;

    // Get bills data for each student - FIXED VERSION
    const studentsWithBills = await Promise.all(
      students.map(async (student) => {
        // FIRST: Check if student has finalized bill
        const [termBill] = await pool.query(
          `
          SELECT * FROM student_term_bills 
          WHERE student_id = ? AND academic_year_id = ? AND term_id = ? AND is_finalized = TRUE
          `,
          [student.student_id, academic_year_id, term_id],
        );

        const hasFinalizedBill = termBill.length > 0;

        if (hasFinalizedBill) {
          // USE FINALIZED BILL DATA - this is our single source of truth
          const finalizedBill = termBill[0];

          // Parse selected_bills to get the actual selected bill IDs and edited amounts
          let selectedBillsData = {};
          try {
            selectedBillsData =
              typeof finalizedBill.selected_bills === "string"
                ? JSON.parse(finalizedBill.selected_bills)
                : finalizedBill.selected_bills;
          } catch (error) {
            console.error("Error parsing selected_bills:", error);
            selectedBillsData = {};
          }

          // Get the actual bill details for selected bills only
          const selectedBillIds = selectedBillsData.bill_ids || [];
          const editedAmounts = selectedBillsData.edited_amounts || {};

          let bills = [];
          let compulsoryBills = [];
          let optionalBills = [];

          if (selectedBillIds.length > 0) {
            [bills] = await pool.query(
              `
                SELECT 
                  b.*,
                  bt.description,
                  bt.is_compulsory,
                  bt.term_id,
                  fc.category_name
                FROM bills b
                LEFT JOIN bill_templates bt ON b.bill_template_id = bt.id
                LEFT JOIN fee_categories fc ON bt.fee_category_id = fc.id
                WHERE b.id IN (?)
                ORDER BY bt.is_compulsory DESC, fc.category_name
                `,
              [selectedBillIds],
            );

            // PROPERLY APPLY EDITED AMOUNTS
            bills = bills.map((bill) => {
              const originalAmount = parseFloat(bill.amount);
              const editedAmount = editedAmounts[bill.id];

              // Use edited amount if it exists, otherwise use original amount
              const finalAmount =
                editedAmount !== undefined
                  ? parseFloat(editedAmount)
                  : originalAmount;

              return {
                ...bill,
                amount: finalAmount, // Override the amount with edited amount
                finalAmount: finalAmount,
                originalAmount: originalAmount,
                isSelected: true, // All bills in finalized bill are selected by definition
                hasCustomAmount:
                  editedAmount !== undefined && editedAmount !== originalAmount,
              };
            });
          }

          // SEPARATE BILLS BY TYPE
          compulsoryBills = bills.filter((bill) => bill.is_compulsory);
          optionalBills = bills.filter((bill) => !bill.is_compulsory);

          // Get arrears and overpayments (use current data, not stored)
          const [arrears] = await pool.query(
            `SELECT * FROM student_arrears 
             WHERE student_id = ? AND (academic_year_id = ? OR academic_year_id IS NULL)`,
            [student.student_id, academic_year_id],
          );

          const [overpayments] = await pool.query(
            `SELECT * FROM student_overpayments 
             WHERE student_id = ? AND status = 'Active' AND (academic_year_id = ? OR academic_year_id IS NULL)`,
            [student.student_id, academic_year_id],
          );

          // Use the STORED totals from finalized bill - don't recalculate!
          const totals = {
            compulsory: parseFloat(finalizedBill.compulsory_amount) || 0,
            optional: parseFloat(finalizedBill.optional_amount) || 0,
            currentTermTotal: parseFloat(finalizedBill.total_amount) || 0,
            arrearsTotal: selectedBillsData.arrears_included || 0,
            overpaymentsTotal: selectedBillsData.overpayments_included || 0,
            total: parseFloat(finalizedBill.total_amount) || 0, // Use the stored total
            isFinalized: true,
          };

          return {
            student,
            bills: bills, // All selected bills
            compulsoryBills: compulsoryBills, // Only compulsory bills
            optionalBills: optionalBills, // Only optional bills
            arrears,
            overpayments,
            termBill: finalizedBill,
            totals,
            isFinalized: true,
          };
        } else {
          // NOT FINALIZED: Show ALL bills (both compulsory and optional) but only calculate compulsory in total
          const [bills] = await pool.query(
            `
            SELECT 
              b.*,
              bt.description,
              bt.is_compulsory,
              bt.term_id,
              fc.category_name
            FROM bills b
            LEFT JOIN bill_templates bt ON b.bill_template_id = bt.id
            LEFT JOIN fee_categories fc ON bt.fee_category_id = fc.id
            WHERE b.student_id = ? AND bt.academic_year_id = ? AND bt.term_id = ?
            ORDER BY bt.is_compulsory DESC, fc.category_name
            `,
            [student.student_id, academic_year_id, term_id],
          );

          // Get arrears and overpayments
          const [arrears] = await pool.query(
            `SELECT * FROM student_arrears 
             WHERE student_id = ? AND (academic_year_id = ? OR academic_year_id IS NULL)`,
            [student.student_id, academic_year_id],
          );

          const [overpayments] = await pool.query(
            `SELECT * FROM student_overpayments 
             WHERE student_id = ? AND status = 'Active' AND (academic_year_id = ? OR academic_year_id IS NULL)`,
            [student.student_id, academic_year_id],
          );

          // Calculate totals from bills table
          // For non-finalized: Only compulsory bills count toward total, but show optional bills
          const compulsoryTotal = bills
            .filter((bill) => bill.is_compulsory)
            .reduce((sum, bill) => sum + parseFloat(bill.amount || 0), 0);

          const optionalTotal = bills
            .filter((bill) => !bill.is_compulsory)
            .reduce((sum, bill) => sum + parseFloat(bill.amount || 0), 0);

          const arrearsTotal = arrears.reduce(
            (sum, arrear) => sum + parseFloat(arrear.amount || 0),
            0,
          );
          const overpaymentsTotal = overpayments.reduce(
            (sum, op) => sum + parseFloat(op.amount || 0),
            0,
          );

          const currentTermTotal = compulsoryTotal; // Only compulsory for non-finalized
          const totalAmount = Math.max(
            currentTermTotal + arrearsTotal - overpaymentsTotal,
            0,
          );

          const totals = {
            compulsory: compulsoryTotal,
            optional: optionalTotal,
            currentTermTotal: currentTermTotal,
            arrearsTotal: arrearsTotal,
            overpaymentsTotal: overpaymentsTotal,
            total: totalAmount,
            isFinalized: false,
          };

          return {
            student,
            bills: bills.map((bill) => ({
              ...bill,
              finalAmount: parseFloat(bill.amount),
              originalAmount: parseFloat(bill.amount),
              isSelected: bill.is_compulsory, // Only compulsory are selected by default for non-finalized
            })),
            compulsoryBills: bills.filter((bill) => bill.is_compulsory),
            optionalBills: bills.filter((bill) => !bill.is_compulsory),
            arrears,
            overpayments,
            termBill: null,
            totals,
            isFinalized: false,
          };
        }
      }),
    );

    // Generate the combined PDF
    const pdfBuffer = await generateCombinedBillsPDFJsPDF(
      studentsWithBills,
      className,
      academic_year_id,
      termName,
    );

    // Send the combined PDF
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${className}-all-bills-${academic_year_id}-${term_id}.pdf"`,
    );

    res.send(pdfBuffer);
  } catch (error) {
    console.error("Error generating class bills PDF:", error);
    res
      .status(500)
      .json({ error: "Failed to generate class bills PDF: " + error.message });
  }
};

// Updated generateCombinedBillsPDFJsPDF function
const generateCombinedBillsPDFJsPDF = async (
  studentsWithBills,
  className,
  academicYearId,
  termName,
) => {
  const { jsPDF } = require("jspdf");

  const pdf = new jsPDF("p", "mm", "a4");
  const pageWidth = pdf.internal.pageSize.getWidth();
  const pageHeight = pdf.internal.pageSize.getHeight();

  // Colors
  const primaryColor = [41, 128, 185];
  const compulsoryColor = [220, 53, 69];
  const optionalColor = [13, 110, 253];
  const arrearsColor = [253, 126, 20];
  const creditColor = [25, 135, 84];
  const darkColor = [33, 37, 41];

  // Get school settings once
  const schoolSettings = await getSchoolSettingsForPDF();

  // Helper function to safely convert values to strings
  const safeText = (value) => {
    if (value === null || value === undefined) return "";
    return String(value);
  };

  // Helper function to safely format numbers
  const safeNumber = (value) => {
    if (value === null || value === undefined) return "0.00";
    const num = parseFloat(value);
    return isNaN(num) ? "0.00" : num.toFixed(2);
  };

  // Process each student
  for (
    let studentIndex = 0;
    studentIndex < studentsWithBills.length;
    studentIndex++
  ) {
    const studentData = studentsWithBills[studentIndex];

    if (studentIndex > 0) {
      pdf.addPage();
    }

    const { student, bills, arrears, overpayments, totals } = studentData;
    let yPosition = 30;

    // ==================== HEADER WITH SCHOOL LOGO ====================
    // Add header for each page
    await addPageHeaderWithLogo(pdf, pageWidth, schoolSettings);

    // Add header line
    pdf.setDrawColor(...primaryColor);
    pdf.setLineWidth(0.5);
    const headerBottomY = 45; // Adjust based on your header height
    pdf.line(15, headerBottomY, pageWidth - 15, headerBottomY);

    // Reset y position after header
    yPosition = headerBottomY + 10;

    // Bill title
    pdf.setFontSize(18);
    pdf.setFont("helvetica", "bold");
    pdf.setTextColor(...primaryColor);
    pdf.text("STUDENT FEE BILL", pageWidth / 2, yPosition, { align: "center" });
    yPosition += 10;

    // ==================== STUDENT INFORMATION ====================
    pdf.setFontSize(10);
    pdf.setFont("helvetica", "normal");
    pdf.setTextColor(0, 0, 0);

    // Student details in two columns
    const leftColumnX = 20;
    const rightColumnX = pageWidth / 2 + 10;

    const studentInfoLeft = [
      ["Student Name:", `${student.first_name} ${student.last_name}`],
      ["Admission No:", student.admission_number],
      ["Class:", student.class_name],
    ];

    const studentInfoRight = [
      ["Academic Year:", academicYearId],
      ["Term:", termName],
      ["Status:", totals.isFinalized ? "FINALIZED" : "DRAFT"],
      ["Date:", new Date().toLocaleDateString()],
    ];

    // Draw left column
    studentInfoLeft.forEach(([label, value], index) => {
      pdf.text(safeText(label), leftColumnX, yPosition);
      pdf.text(safeText(value), leftColumnX + 40, yPosition);
      yPosition += 6;
    });

    // Reset yPosition for right column
    let rightColumnY = yPosition - studentInfoLeft.length * 6;

    // Draw right column
    studentInfoRight.forEach(([label, value]) => {
      pdf.text(safeText(label), rightColumnX, rightColumnY);
      pdf.text(safeText(value), rightColumnX + 35, rightColumnY);
      rightColumnY += 6;
    });

    // Use the lower of the two column positions to continue
    yPosition = Math.max(yPosition, rightColumnY) + 8;

    // ==================== COMPULSORY FEES SECTION ====================
    const compulsoryBills = bills.filter((bill) => bill.is_compulsory);
    if (compulsoryBills && compulsoryBills.length > 0) {
      pdf.setFontSize(12);
      pdf.setFont("helvetica", "bold");
      pdf.setTextColor(...compulsoryColor);
      pdf.text("COMPULSORY FEES", 20, yPosition);
      yPosition += 8;

      pdf.setFontSize(9);
      pdf.setFont("helvetica", "normal");
      pdf.setTextColor(0, 0, 0);

      // Table headers
      pdf.setFillColor(240, 240, 240);
      pdf.rect(20, yPosition, pageWidth - 40, 5, "F");
      pdf.text("Description", 25, yPosition + 3.5);
      pdf.text("Amount (Ghc)", pageWidth - 30, yPosition + 3.5, {
        align: "right",
      });
      yPosition += 9;

      // Compulsory bills
      compulsoryBills.forEach((bill) => {
        if (yPosition > pageHeight - 80) {
          pdf.addPage();
          yPosition = 30;
          // Re-add header for new page
          addPageHeaderWithLogo(pdf, pageWidth, schoolSettings);
          yPosition = 50;
        }

        const amount = bill.finalAmount;
        pdf.text(safeText(bill.category_name), 25, yPosition);
        pdf.text(`Ghc ${safeNumber(amount)}`, pageWidth - 25, yPosition, {
          align: "right",
        });
        yPosition += 4;
      });

      // Compulsory total
      yPosition += 3;
      pdf.setFont("helvetica", "bold");
      pdf.text("Compulsory Total:", pageWidth - 75, yPosition);
      pdf.text(
        `Ghc ${safeNumber(totals.compulsory)}`,
        pageWidth - 25,
        yPosition,
        { align: "right" },
      );
      yPosition += 8;
    }

    // ==================== OPTIONAL FEES SECTION ====================
    const optionalBills = studentData.optionalBills || [];
    const displayOptionalBills = studentData.isFinalized
      ? optionalBills.filter((bill) => bill.isSelected)
      : optionalBills;

    if (displayOptionalBills.length > 0) {
      pdf.setFontSize(12);
      pdf.setFont("helvetica", "bold");
      pdf.setTextColor(...optionalColor);
      pdf.text("OPTIONAL FEES", 20, yPosition);
      yPosition += 8;

      pdf.setFontSize(9);
      pdf.setFont("helvetica", "normal");
      pdf.setTextColor(0, 0, 0);

      // Table headers
      pdf.setFillColor(240, 240, 240);
      pdf.rect(20, yPosition, pageWidth - 40, 5, "F");
      pdf.text("Description", 25, yPosition + 3.5);
      pdf.text("Amount (Ghc)", pageWidth - 30, yPosition + 3.5, {
        align: "right",
      });
      yPosition += 9;

      // Optional bills
      displayOptionalBills.forEach((bill) => {
        if (yPosition > pageHeight - 80) {
          pdf.addPage();
          yPosition = 30;
          // Re-add header for new page
          addPageHeaderWithLogo(pdf, pageWidth, schoolSettings);
          yPosition = 50;
        }

        const amount = bill.finalAmount || parseFloat(bill.amount);
        pdf.text(safeText(bill.category_name), 25, yPosition);
        pdf.text(`Ghc ${safeNumber(amount)}`, pageWidth - 25, yPosition, {
          align: "right",
        });
        yPosition += 4;
      });

      // Optional total
      if (studentData.isFinalized && totals.optional > 0) {
        yPosition += 3;
        pdf.setFont("helvetica", "bold");
        pdf.text("Optional Total:", pageWidth - 70, yPosition);
        pdf.text(
          `Ghc ${safeNumber(totals.optional)}`,
          pageWidth - 25,
          yPosition,
          { align: "right" },
        );
        yPosition += 8;
      } else if (!studentData.isFinalized && displayOptionalBills.length > 0) {
        yPosition += 3;
        pdf.setFont("helvetica", "normal");
        pdf.setTextColor(100, 100, 100);
        pdf.text("(Optional fees not included in total)", 25, yPosition);
        pdf.setTextColor(0, 0, 0);
        yPosition += 6;
      }

      yPosition += 3;
      pdf.setFont("helvetica", "bold");
      pdf.text("Optional Total:", pageWidth - 70, yPosition);
      pdf.text(
        `Ghc ${safeNumber(totals.optional)}`,
        pageWidth - 25,
        yPosition,
        { align: "right" },
      );
      yPosition += 8;
    }

    // ==================== ARREARS SECTION ====================
    if (arrears.length > 0 || totals.arrearsTotal > 0) {
      pdf.setFontSize(12);
      pdf.setFont("helvetica", "bold");
      pdf.setTextColor(...arrearsColor);
      pdf.text("OUTSTANDING ARREARS", 20, yPosition);
      yPosition += 8;

      pdf.setFontSize(9);
      pdf.setFont("helvetica", "normal");
      pdf.setTextColor(0, 0, 0);

      if (arrears.length > 0) {
        arrears.forEach((arrear) => {
          if (yPosition > pageHeight - 60) {
            pdf.addPage();
            yPosition = 30;
            // Re-add header for new page
            addPageHeaderWithLogo(pdf, pageWidth, schoolSettings);
            yPosition = 50;
          }

          pdf.text(safeText(arrear.description), 25, yPosition);
          pdf.text(
            `Ghc ${safeNumber(arrear.amount)}`,
            pageWidth - 25,
            yPosition,
            { align: "right" },
          );
          yPosition += 5;
        });
      } else {
        if (yPosition > pageHeight - 60) {
          pdf.addPage();
          yPosition = 30;
          // Re-add header for new page
          addPageHeaderWithLogo(pdf, pageWidth, schoolSettings);
          yPosition = 50;
        }
        pdf.text("Previous Balance", 25, yPosition);
        pdf.text(
          `Ghc ${safeNumber(totals.arrearsTotal)}`,
          pageWidth - 25,
          yPosition,
          { align: "right" },
        );
        yPosition += 5;
      }

      yPosition += 3;
      pdf.setFont("helvetica", "bold");
      pdf.text("Total Arrears:", pageWidth - 70, yPosition);
      pdf.text(
        `Ghc ${safeNumber(totals.arrearsTotal)}`,
        pageWidth - 25,
        yPosition,
        { align: "right" },
      );
      yPosition += 8;
    }

    // ==================== CREDITS SECTION ====================
    if (overpayments.length > 0 || totals.overpaymentsTotal > 0) {
      pdf.setFontSize(12);
      pdf.setFont("helvetica", "bold");
      pdf.setTextColor(...creditColor);
      pdf.text("AVAILABLE CREDITS", 20, yPosition);
      yPosition += 8;

      pdf.setFontSize(9);
      pdf.setFont("helvetica", "normal");
      pdf.setTextColor(0, 0, 0);

      if (overpayments.length > 0) {
        overpayments.forEach((overpayment) => {
          if (yPosition > pageHeight - 60) {
            pdf.addPage();
            yPosition = 30;
            // Re-add header for new page
            addPageHeaderWithLogo(pdf, pageWidth, schoolSettings);
            yPosition = 50;
          }

          pdf.text(safeText(overpayment.description), 25, yPosition);
          pdf.text(
            `-Ghc ${safeNumber(overpayment.amount)}`,
            pageWidth - 25,
            yPosition,
            { align: "right" },
          );
          yPosition += 5;
        });
      } else {
        if (yPosition > pageHeight - 60) {
          pdf.addPage();
          yPosition = 30;
          // Re-add header for new page
          addPageHeaderWithLogo(pdf, pageWidth, schoolSettings);
          yPosition = 50;
        }
        pdf.text("Available Credit Balance", 25, yPosition);
        pdf.text(
          `-Ghc ${safeNumber(totals.overpaymentsTotal)}`,
          pageWidth - 25,
          yPosition,
          { align: "right" },
        );
        yPosition += 5;
      }

      yPosition += 3;
      pdf.setFont("helvetica", "bold");
      pdf.text("Total Credits:", pageWidth - 70, yPosition);
      pdf.text(
        `-Ghc ${safeNumber(totals.overpaymentsTotal)}`,
        pageWidth - 25,
        yPosition,
        { align: "right" },
      );
      yPosition += 8;
    }

    // ==================== FINAL TOTAL ====================
    yPosition += 5;
    pdf.setFillColor(...darkColor);
    pdf.rect(20, yPosition, pageWidth - 40, 10, "F");
    pdf.setTextColor(255, 255, 255);
    pdf.setFontSize(12);
    pdf.setFont("helvetica", "bold");

    if (totals.total > 0) {
      pdf.text("TOTAL AMOUNT DUE:", 25, yPosition + 6);
      pdf.text(
        `Ghc ${safeNumber(totals.total)}`,
        pageWidth - 25,
        yPosition + 6,
        { align: "right" },
      );
    } else {
      pdf.text("FULLY COVERED BY CREDITS", pageWidth / 2, yPosition + 6, {
        align: "center",
      });
    }

    yPosition += 20;

    // ==================== FOOTER ====================
    pdf.setTextColor(100, 100, 100);
    pdf.setFontSize(7);
    pdf.setFont("helvetica", "normal");

    // School bank details if available
    if (schoolSettings.bank_name && schoolSettings.account_number) {
      let bankInfo = `Bank: ${schoolSettings.bank_name}`;
      if (schoolSettings.account_name) {
        bankInfo += ` | A/C Name: ${schoolSettings.account_name}`;
      }
      bankInfo += ` | A/C No: ${schoolSettings.account_number}`;

      const bankLines = pdf.splitTextToSize(bankInfo, pageWidth - 30);
      bankLines.forEach((line, index) => {
        pdf.text(line, pageWidth / 2, pageHeight - 20 + index * 3, {
          align: "center",
        });
      });
    }

    pdf.text(
      `This is a ${
        totals.isFinalized ? "finalized" : "draft"
      } bill • Generated on ${new Date().toLocaleDateString()}`,
      pageWidth / 2,
      pageHeight - 15,
      { align: "center" },
    );
    pdf.text(
      `${schoolSettings.school_name} - Official Fee Bill`,
      pageWidth / 2,
      pageHeight - 10,
      { align: "center" },
    );
  }

  // Return PDF as buffer
  return Buffer.from(pdf.output("arraybuffer"));
};

// ==================== UPDATED HEADER FUNCTION ====================
const addPageHeaderWithLogo = async (pdf, pageWidth, schoolSettings = null) => {
  // Get school settings if not provided
  if (!schoolSettings) {
    schoolSettings = await getSchoolSettingsForPDF();
  }

  const primaryColor = [41, 128, 185];
  const headerY = 10;

  // Try to add school logo (top left, 20x20mm)
  const hasLogo = await addSchoolLogoToPDF(pdf, 15, headerY, 20, 20);

  // School name and info (position based on logo presence)
  const schoolNameX = hasLogo ? 40 : 20;

  // School name
  pdf.setFontSize(hasLogo ? 14 : 16);
  pdf.setFont("helvetica", "bold");
  pdf.setTextColor(...primaryColor);
  pdf.text(schoolSettings.school_name, schoolNameX, headerY + 5);

  // School motto if exists
  if (schoolSettings.motto) {
    pdf.setFontSize(10);
    pdf.setFont("helvetica", "italic");
    pdf.setTextColor(100, 100, 100);
    pdf.text(schoolSettings.motto, schoolNameX, headerY + 10);
  }

  // School address
  pdf.setFontSize(9);
  pdf.setFont("helvetica", "normal");
  pdf.setTextColor(0, 0, 0);

  // Build address lines
  let addressLine = schoolSettings.address || "";
  if (schoolSettings.city) addressLine += `, ${schoolSettings.city}`;
  if (schoolSettings.region) addressLine += `, ${schoolSettings.region}`;

  // Split address if too long
  const addressLines = pdf.splitTextToSize(
    addressLine,
    pageWidth - schoolNameX - 20,
  );
  addressLines.forEach((line, index) => {
    pdf.text(line, schoolNameX, headerY + 15 + index * 4);
  });

  // Contact info
  let contactY = headerY + 15 + addressLines.length * 4 + 2;
  let contactInfo = "";

  if (schoolSettings.phone_numbers && schoolSettings.phone_numbers.length > 0) {
    // Parse phone numbers if it's a JSON string
    let phones = schoolSettings.phone_numbers;
    if (typeof phones === "string") {
      try {
        phones = JSON.parse(phones);
      } catch (e) {
        phones = [phones];
      }
    }

    if (Array.isArray(phones) && phones.length > 0) {
      contactInfo = `Phone: ${phones[0]}`;
      if (phones.length > 1) {
        contactInfo += ` / ${phones[1]}`;
      }
    }
  }

  if (schoolSettings.email) {
    if (contactInfo) contactInfo += " • ";
    contactInfo += `Email: ${schoolSettings.email}`;
  }

  if (contactInfo) {
    pdf.text(contactInfo, schoolNameX, contactY);
  }

  // Add a decorative line
  pdf.setDrawColor(...primaryColor);
  pdf.setLineWidth(0.5);
  pdf.line(15, contactY + 4, pageWidth - 15, contactY + 4);

  pdf.setTextColor(0, 0, 0);
};

//fetch students by class
// Add this function to control.js
const getStudentsByClass = async (req, res) => {
  try {
    const { class_id } = req.query;

    if (!class_id) {
      return res.status(400).json({ error: "Class ID is required" });
    }

    // Get current academic year
    const [currentYear] = await pool.query(
      "SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1",
    );

    if (currentYear.length === 0) {
      return res.status(400).json({ error: "No current academic year set" });
    }

    const academicYearId = currentYear[0].id;

    // Query to get students in specific class for current academic year
    const [students] = await pool.query(
      `
      SELECT 
        s.id,
        s.admission_number,
        s.first_name,
        s.last_name,
        s.date_of_birth,
        s.gender,
        s.parent_name,
        s.parent_contact,
        s.address,
        s.enrolled_date,
        s.has_fee_block,
        s.is_active,
        s.photo_filename,
        c.class_name,
        c.room_number,
        ca.promotion_status,
        ca.date_assigned as class_assignment_date
      FROM students s
      INNER JOIN class_assignments ca ON s.id = ca.student_id
      INNER JOIN classes c ON ca.class_id = c.id
      WHERE ca.class_id = ?
        AND ca.academic_year_id = ?
        AND (s.is_active IS NULL OR s.is_active = TRUE)
      ORDER BY s.first_name, s.last_name
    `,
      [class_id, academicYearId],
    );

    res.json(students);
  } catch (error) {
    console.error("Error fetching students by class:", error);
    res.status(500).json({ error: "Failed to fetch students by class" });
  }
};

// CORRECTED Process Payment Function - Uses finalized bill as source of truth

// const processPayment = async (req, res) => {
//   const connection = await pool.getConnection();

//   try {
//     await connection.beginTransaction();

//     const {
//       student_id,
//       amount_paid,
//       payment_date,
//       payment_method,
//       reference_number,
//       received_by,
//       notes,
//       allocations,
//       bill_descriptions,
//     } = req.body;

//     // If student_id is undefined, check if it's named differently
//     if (!student_id) {
//       console.error("student_id is undefined in req.body");
//       console.error("Available keys in req.body:", Object.keys(req.body));
//       throw new Error("student_id is required");
//     }

//     // 1. Get current academic context
//     const [currentYear] = await connection.query(
//       "SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1"
//     );

//     const [currentTerm] = await connection.query(
//       `SELECT id FROM terms
//        WHERE start_date <= CURDATE()
//        AND end_date >= CURDATE()
//        LIMIT 1`
//     );

//     const academicYearId = currentYear[0]?.id;
//     const termId = currentTerm[0]?.id;

//     if (!academicYearId || !termId) {
//       await connection.rollback();
//       return res.status(400).json({
//         error: "Could not determine current academic year or term",
//       });
//     }

//     // 2. Get finalized bill WITH SELECTED BILLS DATA
//     const [finalizedBills] = await connection.query(
//       `SELECT * FROM student_term_bills
//        WHERE student_id = ?
//          AND academic_year_id = ?
//          AND term_id = ?
//          AND is_finalized = TRUE
//        FOR UPDATE`,
//       [student_id, academicYearId, termId]
//     );

//     if (finalizedBills.length === 0) {
//       await connection.rollback();
//       return res.status(400).json({
//         error: "Student does not have a finalized bill for the current term.",
//       });
//     }

//     const finalizedBill = finalizedBills[0];

//     // Parse selected bills data
//     let selectedBillsData = {};
//     try {
//       selectedBillsData =
//         typeof finalizedBill.selected_bills === "string"
//           ? JSON.parse(finalizedBill.selected_bills)
//           : finalizedBill.selected_bills;
//     } catch (error) {
//       console.error("Error parsing selected_bills:", error);
//       selectedBillsData = {};
//     }

//     const selectedBillIds = selectedBillsData.bill_ids || [];
//     const editedAmounts = selectedBillsData.edited_amounts || {};

//     // 3. Get detailed bill information for selected bills
//     const [selectedBills] = await connection.query(
//       `SELECT b.*, bt.is_compulsory, bt.fee_category_id
//        FROM bills b
//        JOIN bill_templates bt ON b.bill_template_id = bt.id
//        WHERE b.id IN (?)
//        FOR UPDATE`,
//       [selectedBillIds]
//     );

//     // 4. Calculate REAL remaining amounts based on finalized data
//     const billDetails = selectedBills.map((bill) => {
//       const editedAmount = editedAmounts[bill.id];
//       const finalizedAmount =
//         editedAmount !== undefined
//           ? parseFloat(editedAmount)
//           : parseFloat(bill.amount);

//       const paidSoFar = parseFloat(bill.paid_amount || 0);
//       const realRemaining = Math.max(0, finalizedAmount - paidSoFar);

//       return {
//         ...bill,
//         finalized_amount: finalizedAmount,
//         real_remaining: realRemaining,
//         original_amount: parseFloat(bill.amount),
//       };
//     });

//     // 5. Validate allocations don't exceed REAL remaining amounts
//     let totalAllocated = 0;
//     const validationErrors = [];

//     for (const [billId, allocatedAmount] of Object.entries(allocations)) {
//       const allocatedNum = parseFloat(allocatedAmount) || 0;

//       if (allocatedNum > 0) {
//         const billDetail = billDetails.find((b) => b.id == billId);

//         if (!billDetail) {
//           validationErrors.push(`Bill ${billId} not found in selected bills`);
//           continue;
//         }

//         const availableBalance = billDetail.real_remaining;

//         if (allocatedNum > availableBalance + 0.01) {
//           // Allow small rounding differences
//           validationErrors.push({
//             bill_id: billId,
//             allocated: allocatedNum,
//             available: availableBalance,
//             message: `Allocation (Ghc ${allocatedNum.toFixed(
//               2
//             )}) exceeds real remaining balance (Ghc ${availableBalance.toFixed(
//               2
//             )})`,
//           });
//         }

//         totalAllocated += allocatedNum;
//       }
//     }

//     if (validationErrors.length > 0) {
//       console.error("Allocation validation errors:", validationErrors);
//       await connection.rollback();
//       return res.status(400).json({
//         error: "Allocation validation failed",
//         details: validationErrors,
//       });
//     }

//     // 6. Validate total allocation matches payment amount (with tolerance for rounding)
//     const allocationDifference = Math.abs(totalAllocated - amount_paid);
//     if (allocationDifference > 0.01) {
//       // Allow 1 cent difference for rounding
//       await connection.rollback();
//       return res.status(400).json({
//         error: `Total allocated amount (Ghc ${totalAllocated.toFixed(
//           2
//         )}) does not match payment amount (Ghc ${amount_paid.toFixed(2)})`,
//       });
//     }
//     // 7. Create payment record
//     const [paymentResult] = await connection.query(
//       `INSERT INTO payments (student_id, amount_paid, payment_date, payment_method, reference_number, received_by, notes)
//        VALUES (?, ?, ?, ?, ?, ?, ?)`,
//       [
//         student_id,
//         amount_paid,
//         payment_date,
//         payment_method,
//         reference_number,
//         received_by,
//         notes,
//       ]
//     );

//     const paymentId = paymentResult.insertId;

//     // 8. Update individual bills with REAL remaining calculation
//     for (const [billId, allocatedAmount] of Object.entries(allocations)) {
//       const allocatedNum = parseFloat(allocatedAmount) || 0;

//       if (allocatedNum > 0) {
//         const billDetail = billDetails.find((b) => b.id == billId);

//         if (billDetail) {
//           // Get description for this bill
//           const description = bill_descriptions
//             ? bill_descriptions[billId]
//             : null;

//           // Calculate new paid amount
//           const currentPaid = parseFloat(billDetail.paid_amount || 0);
//           const newPaidAmount = currentPaid + allocatedNum;
//           const finalizedAmount = billDetail.finalized_amount;

//           // Determine payment status based on REAL remaining
//           const remainingAfterPayment = Math.max(
//             0,
//             finalizedAmount - newPaidAmount
//           );
//           let paymentStatus = "Partially Paid";

//           if (remainingAfterPayment <= 0.01) {
//             // Fully paid if less than 1 cedi
//             paymentStatus = "Paid";
//           } else if (newPaidAmount <= 0) {
//             paymentStatus = "Pending";
//           }

//           // Update bill with correct amounts
//           await connection.query(
//             `UPDATE bills SET
//               paid_amount = ?,
//               remaining_amount = ?,
//               payment_status = ?
//              WHERE id = ?`,
//             [newPaidAmount, remainingAfterPayment, paymentStatus, billId]
//           );

//           // Create payment allocation
//           await connection.query(
//             `INSERT INTO payment_allocations (payment_id, bill_id, amount_allocated, description)
//              VALUES (?, ?, ?, ?)`,
//             [paymentId, billId, allocatedNum, description]
//           );
//         }
//       }
//     }

//     // 9. Update finalized bill balance
//     const currentPaidTotal = parseFloat(finalizedBill.paid_amount || 0);
//     const newPaidTotal = currentPaidTotal + amount_paid;
//     const totalAmount = parseFloat(finalizedBill.total_amount);
//     const newRemainingBalance = Math.max(0, totalAmount - newPaidTotal);
//     const isFullyPaid = newRemainingBalance <= 0.01; // Within 1 cent tolerance

//     await connection.query(
//       `UPDATE student_term_bills SET
//         paid_amount = ?,
//         remaining_balance = ?,
//         is_fully_paid = ?,
//         last_payment_date = ?
//        WHERE id = ?`,
//       [
//         newPaidTotal,
//         newRemainingBalance,
//         isFullyPaid,
//         payment_date,
//         finalizedBill.id,
//       ]
//     );

//     // 10. Generate receipt
//     const receiptNumber = `RCP-${Date.now()}-${paymentId}`;

//     const [receiptResult] = await connection.query(
//       `INSERT INTO receipts (receipt_number, student_id, payment_id, issued_date, issued_by, academic_year_id, term_id, notes)
//        VALUES (?, ?, ?, CURDATE(), ?, ?, ?, ?)`,
//       [
//         receiptNumber,
//         student_id,
//         paymentId,
//         received_by,
//         academicYearId,
//         termId,
//         notes,
//       ]
//     );

//     await connection.commit();
//     try {
//       // Get student details with parent email
//       const [studentDetails] = await connection.query(
//         `SELECT
//            s.first_name,
//            s.last_name,
//            s.admission_number,
//            s.parent_name,
//            s.parent_contact,
//            s.parent_email,
//            c.class_name
//          FROM students s
//          LEFT JOIN class_assignments ca ON s.id = ca.student_id AND ca.academic_year_id = ?
//          LEFT JOIN classes c ON ca.class_id = c.id
//          WHERE s.id = ?`,
//         [academicYearId, student_id],
//       );

//       const student = studentDetails[0];

//       // Send email if we have an address
//       if (student && (student.parent_email || student.student_email)) {
//         const emailService = require("../utils/emailServices");

//         // Don't await - let it run in background
//         emailService
//           .sendPaymentReceipt(
//             {
//               amount_paid,
//               payment_method,
//               payment_date,
//               reference_number,
//             },
//             {
//               ...student,
//               parent_name: student.parent_name || "Parent",
//             },
//             receiptNumber,
//           )
//           .then((result) => {
//             if (result.success) {
//               console.log(
//                 `Email sent to ${student.parent_email || student.student_email}`,
//               );
//             } else {
//               console.log("Email not sent:", result.message);
//             }
//           })
//           .catch((err) => {
//             console.error("Background email error:", err);
//           });
//       }
//     } catch (emailError) {
//       // Log but don't fail the payment
//       console.error("Error preparing email notification:", emailError);
//     }
//     clearRelevantCaches("PROCESS_PAYMENT", {
//       student_id: student_id,
//       academic_year_id: academicYearId,
//       term_id: termId,
//     });

//     // 11. Get updated bill status for response
//     const [updatedBills] = await connection.query(
//       `SELECT id, description, amount, paid_amount, remaining_amount, payment_status
//        FROM bills WHERE id IN (?)`,
//       [Object.keys(allocations).filter((id) => allocations[id] > 0)]
//     );

//     res.json({
//       success: true,
//       message: `Payment of Ghc ${amount_paid.toFixed(
//         2
//       )} processed successfully.`,
//       receipt: {
//         receipt_number: receiptNumber,
//         payment_id: paymentId,
//         amount: amount_paid,
//       },
//       balance: {
//         previous_balance: parseFloat(
//           finalizedBill.remaining_balance || totalAmount
//         ),
//         paid: amount_paid,
//         new_balance: newRemainingBalance,
//         is_fully_paid: isFullyPaid,
//       },
//       bill_updates: updatedBills,
//       term_bill: {
//         id: finalizedBill.id,
//         total_amount: totalAmount,
//         new_paid_amount: newPaidTotal,
//         new_remaining_balance: newRemainingBalance,
//       },
//     });
//   } catch (error) {
//     await connection.rollback();
//     clearRelevantCaches("PROCESS_PAYMENT", {
//       student_id: student_id,
//       academic_year_id: academicYearId,
//       term_id: termId,
//     });
//     console.error("Error processing payment:", error);
//     res.status(500).json({
//       error: "Failed to process payment: " + error.message,
//       stack: process.env.NODE_ENV === "development" ? error.stack : undefined,
//     });
//   } finally {
//     connection.release();
//   }
// };

// Process Payment Function - With Email Notifications

// 2nd process paymenet function which was working correctly
// const processPayment = async (req, res) => {
//   const connection = await pool.getConnection();

//   try {
//     await connection.beginTransaction();

//     const {
//       student_id,
//       amount_paid,
//       payment_date,
//       payment_method,
//       reference_number,
//       received_by,
//       notes,
//       allocations,
//       bill_descriptions,
//     } = req.body;

//     console.log("Processing payment for student_id:", student_id);

//     if (!student_id) {
//       console.error("student_id is undefined in req.body");
//       throw new Error("student_id is required");
//     }

//     // 1. Get current academic context
//     const [currentYear] = await connection.query(
//       "SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1",
//     );

//     const [currentTerm] = await connection.query(
//       `SELECT id FROM terms
//        WHERE start_date <= CURDATE()
//        AND end_date >= CURDATE()
//        LIMIT 1`,
//     );

//     const academicYearId = currentYear[0]?.id;
//     const termId = currentTerm[0]?.id;

//     if (!academicYearId || !termId) {
//       await connection.rollback();
//       return res.status(400).json({
//         error: "Could not determine current academic year or term",
//       });
//     }

//     // 2. Get finalized bill
//     const [finalizedBills] = await connection.query(
//       `SELECT * FROM student_term_bills
//        WHERE student_id = ?
//          AND academic_year_id = ?
//          AND term_id = ?
//          AND is_finalized = TRUE
//        FOR UPDATE`,
//       [student_id, academicYearId, termId],
//     );

//     if (finalizedBills.length === 0) {
//       await connection.rollback();
//       return res.status(400).json({
//         error: "Student does not have a finalized bill for the current term.",
//       });
//     }

//     const finalizedBill = finalizedBills[0];

//     // Parse selected bills data
//     let selectedBillsData = {};
//     try {
//       selectedBillsData =
//         typeof finalizedBill.selected_bills === "string"
//           ? JSON.parse(finalizedBill.selected_bills)
//           : finalizedBill.selected_bills;
//     } catch (error) {
//       console.error("Error parsing selected_bills:", error);
//       selectedBillsData = {};
//     }

//     const selectedBillIds = selectedBillsData.bill_ids || [];
//     const editedAmounts = selectedBillsData.edited_amounts || {};

//     // 3. Get detailed bill information
//     const [selectedBills] = await connection.query(
//       `SELECT b.*, bt.is_compulsory, bt.fee_category_id
//        FROM bills b
//        JOIN bill_templates bt ON b.bill_template_id = bt.id
//        WHERE b.id IN (?)
//        FOR UPDATE`,
//       [selectedBillIds],
//     );

//     // 4. Calculate real remaining amounts
//     const billDetails = selectedBills.map((bill) => {
//       const editedAmount = editedAmounts[bill.id];
//       const finalizedAmount =
//         editedAmount !== undefined
//           ? parseFloat(editedAmount)
//           : parseFloat(bill.amount);

//       const paidSoFar = parseFloat(bill.paid_amount || 0);
//       const realRemaining = Math.max(0, finalizedAmount - paidSoFar);

//       return {
//         ...bill,
//         finalized_amount: finalizedAmount,
//         real_remaining: realRemaining,
//         original_amount: parseFloat(bill.amount),
//       };
//     });

//     // 5. Validate allocations
//     let totalAllocated = 0;
//     const validationErrors = [];

//     for (const [billId, allocatedAmount] of Object.entries(allocations)) {
//       const allocatedNum = parseFloat(allocatedAmount) || 0;

//       if (allocatedNum > 0) {
//         const billDetail = billDetails.find((b) => b.id == billId);

//         if (!billDetail) {
//           validationErrors.push(`Bill ${billId} not found in selected bills`);
//           continue;
//         }

//         const availableBalance = billDetail.real_remaining;

//         if (allocatedNum > availableBalance + 0.01) {
//           validationErrors.push({
//             bill_id: billId,
//             allocated: allocatedNum,
//             available: availableBalance,
//             message: `Allocation (Ghc ${allocatedNum.toFixed(
//               2,
//             )}) exceeds real remaining balance (Ghc ${availableBalance.toFixed(
//               2,
//             )})`,
//           });
//         }

//         totalAllocated += allocatedNum;
//       }
//     }

//     if (validationErrors.length > 0) {
//       await connection.rollback();
//       return res.status(400).json({
//         error: "Allocation validation failed",
//         details: validationErrors,
//       });
//     }

//     // 6. Validate total allocation matches payment amount
//     const allocationDifference = Math.abs(totalAllocated - amount_paid);
//     if (allocationDifference > 0.01) {
//       await connection.rollback();
//       return res.status(400).json({
//         error: `Total allocated amount (Ghc ${totalAllocated.toFixed(
//           2,
//         )}) does not match payment amount (Ghc ${amount_paid.toFixed(2)})`,
//       });
//     }

//     // 7. Create payment record
//     const [paymentResult] = await connection.query(
//       `INSERT INTO payments (student_id, amount_paid, payment_date, payment_method, reference_number, received_by, notes)
//        VALUES (?, ?, ?, ?, ?, ?, ?)`,
//       [
//         student_id,
//         amount_paid,
//         payment_date,
//         payment_method,
//         reference_number,
//         received_by,
//         notes,
//       ],
//     );

//     const paymentId = paymentResult.insertId;

//     // 8. Update individual bills
//     for (const [billId, allocatedAmount] of Object.entries(allocations)) {
//       const allocatedNum = parseFloat(allocatedAmount) || 0;

//       if (allocatedNum > 0) {
//         const billDetail = billDetails.find((b) => b.id == billId);

//         if (billDetail) {
//           const description = bill_descriptions
//             ? bill_descriptions[billId]
//             : null;

//           const currentPaid = parseFloat(billDetail.paid_amount || 0);
//           const newPaidAmount = currentPaid + allocatedNum;
//           const finalizedAmount = billDetail.finalized_amount;

//           const remainingAfterPayment = Math.max(
//             0,
//             finalizedAmount - newPaidAmount,
//           );
//           let paymentStatus = "Partially Paid";

//           if (remainingAfterPayment <= 0.01) {
//             paymentStatus = "Paid";
//           } else if (newPaidAmount <= 0) {
//             paymentStatus = "Pending";
//           }

//           await connection.query(
//             `UPDATE bills SET
//               paid_amount = ?,
//               remaining_amount = ?,
//               payment_status = ?
//              WHERE id = ?`,
//             [newPaidAmount, remainingAfterPayment, paymentStatus, billId],
//           );

//           await connection.query(
//             `INSERT INTO payment_allocations (payment_id, bill_id, amount_allocated, description)
//              VALUES (?, ?, ?, ?)`,
//             [paymentId, billId, allocatedNum, description],
//           );
//         }
//       }
//     }

//     // 9. Update finalized bill balance
//     const currentPaidTotal = parseFloat(finalizedBill.paid_amount || 0);
//     const newPaidTotal = currentPaidTotal + amount_paid;
//     const totalAmount = parseFloat(finalizedBill.total_amount);
//     const newRemainingBalance = Math.max(0, totalAmount - newPaidTotal);
//     const isFullyPaid = newRemainingBalance <= 0.01;

//     await connection.query(
//       `UPDATE student_term_bills SET
//         paid_amount = ?,
//         remaining_balance = ?,
//         is_fully_paid = ?,
//         last_payment_date = ?
//        WHERE id = ?`,
//       [
//         newPaidTotal,
//         newRemainingBalance,
//         isFullyPaid,
//         payment_date,
//         finalizedBill.id,
//       ],
//     );

//     // 10. Generate receipt
//     const receiptNumber = `RCP-${Date.now()}-${paymentId}`;

//     const [receiptResult] = await connection.query(
//       `INSERT INTO receipts (receipt_number, student_id, payment_id, issued_date, issued_by, academic_year_id, term_id, notes)
//        VALUES (?, ?, ?, CURDATE(), ?, ?, ?, ?)`,
//       [
//         receiptNumber,
//         student_id,
//         paymentId,
//         received_by,
//         academicYearId,
//         termId,
//         notes,
//       ],
//     );

//     await connection.commit();

//     // ============ SEND EMAIL NOTIFICATION ============
//     let emailStatus = {
//       sent: false,
//       message: "No email address available",
//       recipient: null,
//     };

//     try {
//       // Get student details with parent email
//       const [studentDetails] = await connection.query(
//         `SELECT
//            s.id,
//            s.first_name,
//            s.last_name,
//            s.admission_number,
//            s.parent_name,
//            s.parent_contact,
//            s.parent_email,
//            c.class_name
//          FROM students s
//          LEFT JOIN class_assignments ca ON s.id = ca.student_id AND ca.academic_year_id = ?
//          LEFT JOIN classes c ON ca.class_id = c.id
//          WHERE s.id = ?`,
//         [academicYearId, student_id],
//       );

//       const student = studentDetails[0];

//       if (student && (student.parent_email || student.student_email)) {
//         const recipientEmail = student.parent_email || student.student_email;
//         emailStatus.recipient = recipientEmail;
//         emailStatus.message = "Sending email...";

//         const emailService = require("../utils/emailServices");

//         // Send email in background - but capture promise to await if needed
//         emailService
//           .sendPaymentReceipt(
//             {
//               amount_paid,
//               payment_method,
//               payment_date,
//               reference_number,
//               payment_id: paymentId,
//             },
//             student,
//             receiptNumber,
//           )
//           .then(async (result) => {
//             if (result.success) {
//               console.log(`✅ Email sent to ${recipientEmail}`);
//               emailStatus.sent = true;
//               emailStatus.message = "Email sent successfully";

//               // Log to database
//               try {
//                 await pool.query(
//                   `INSERT INTO email_logs
//                  (student_id, email_type, recipient_email, status, message_id, sent_at)
//                  VALUES (?, 'payment_receipt', ?, 'sent', ?, NOW())`,
//                   [student.id, recipientEmail, result.messageId],
//                 );
//               } catch (logError) {
//                 console.error("Error logging email:", logError);
//               }
//             } else {
//               console.log(`⚠️ Email failed: ${result.message}`);
//               emailStatus.sent = false;
//               emailStatus.message = `Email failed: ${result.message}`;

//               // Log failure
//               try {
//                 await pool.query(
//                   `INSERT INTO email_logs
//                  (student_id, email_type, recipient_email, status, error_message, sent_at)
//                  VALUES (?, 'payment_receipt', ?, 'failed', ?, NOW())`,
//                   [student.id, recipientEmail, result.message],
//                 );
//               } catch (logError) {
//                 console.error("Error logging failed email:", logError);
//               }
//             }
//           })
//           .catch((err) => {
//             console.error("❌ Background email error:", err);
//             emailStatus.sent = false;
//             emailStatus.message = `Email error: ${err.message}`;
//           });
//       } else {
//         emailStatus.message = "No email address on file";
//         console.log(`📭 No email address for student ${student_id}`);
//       }
//     } catch (emailError) {
//       console.error("Error preparing email notification:", emailError);
//       emailStatus.message = `Error: ${emailError.message}`;
//     }

//     // Clear caches
//     clearRelevantCaches("PROCESS_PAYMENT", {
//       student_id: student_id,
//       academic_year_id: academicYearId,
//       term_id: termId,
//     });

//     // 11. Get updated bill status for response
//     const [updatedBills] = await connection.query(
//       `SELECT id, description, amount, paid_amount, remaining_amount, payment_status
//        FROM bills WHERE id IN (?)`,
//       [Object.keys(allocations).filter((id) => allocations[id] > 0)],
//     );

//     // ============ SEND RESPONSE ============
//     res.json({
//       success: true,
//       message: `Payment of Ghc ${amount_paid.toFixed(
//         2,
//       )} processed successfully.`,
//       receipt: {
//         receipt_number: receiptNumber,
//         payment_id: paymentId,
//         amount: amount_paid,
//       },
//       balance: {
//         previous_balance: parseFloat(
//           finalizedBill.remaining_balance || totalAmount,
//         ),
//         paid: amount_paid,
//         new_balance: newRemainingBalance,
//         is_fully_paid: isFullyPaid,
//       },
//       bill_updates: updatedBills,
//       term_bill: {
//         id: finalizedBill.id,
//         total_amount: totalAmount,
//         new_paid_amount: newPaidTotal,
//         new_remaining_balance: newRemainingBalance,
//       },
//       email: emailStatus,
//     });
//   } catch (error) {
//     await connection.rollback();
//     clearRelevantCaches("PROCESS_PAYMENT");
//     console.error("Error processing payment:", error);
//     res.status(500).json({
//       error: "Failed to process payment: " + error.message,
//       stack: process.env.NODE_ENV === "development" ? error.stack : undefined,
//     });
//   } finally {
//     connection.release();
//   }
// };

// Generate Receipt PDF

const processPayment = async (req, res) => {
  const connection = await pool.getConnection();

  try {
    await connection.beginTransaction();

    const {
      student_id,
      amount_paid,
      payment_date,
      payment_method,
      reference_number,
      received_by,
      notes,
      allocations,
      bill_descriptions,
    } = req.body;

    console.log("Processing payment for student_id:", student_id);

    if (!student_id) {
      console.error("student_id is undefined in req.body");
      throw new Error("student_id is required");
    }

    // 1. Get current academic context
    const [currentYear] = await connection.query(
      "SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1",
    );

    const [currentTerm] = await connection.query(
      `SELECT id FROM terms 
       WHERE start_date <= CURDATE() 
       AND end_date >= CURDATE() 
       LIMIT 1`,
    );

    const academicYearId = currentYear[0]?.id;
    const termId = currentTerm[0]?.id;

    if (!academicYearId || !termId) {
      await connection.rollback();
      return res.status(400).json({
        error: "Could not determine current academic year or term",
      });
    }

    // 2. Get finalized bill
    const [finalizedBills] = await connection.query(
      `SELECT * FROM student_term_bills 
       WHERE student_id = ? 
         AND academic_year_id = ? 
         AND term_id = ? 
         AND is_finalized = TRUE
       FOR UPDATE`,
      [student_id, academicYearId, termId],
    );

    if (finalizedBills.length === 0) {
      await connection.rollback();
      return res.status(400).json({
        error: "Student does not have a finalized bill for the current term.",
      });
    }

    const finalizedBill = finalizedBills[0];

    // Parse selected bills data
    let selectedBillsData = {};
    try {
      selectedBillsData =
        typeof finalizedBill.selected_bills === "string"
          ? JSON.parse(finalizedBill.selected_bills)
          : finalizedBill.selected_bills;
    } catch (error) {
      console.error("Error parsing selected_bills:", error);
      selectedBillsData = {};
    }

    const selectedBillIds = selectedBillsData.bill_ids || [];
    const editedAmounts = selectedBillsData.edited_amounts || {};

    // 3. Get detailed bill information
    const [selectedBills] = await connection.query(
      `SELECT b.*, bt.is_compulsory, bt.fee_category_id
       FROM bills b
       JOIN bill_templates bt ON b.bill_template_id = bt.id
       WHERE b.id IN (?)
       FOR UPDATE`,
      [selectedBillIds],
    );

    // 4. Calculate real remaining amounts
    const billDetails = selectedBills.map((bill) => {
      const editedAmount = editedAmounts[bill.id];
      const finalizedAmount =
        editedAmount !== undefined
          ? parseFloat(editedAmount)
          : parseFloat(bill.amount);

      const paidSoFar = parseFloat(bill.paid_amount || 0);
      const realRemaining = Math.max(0, finalizedAmount - paidSoFar);

      return {
        ...bill,
        finalized_amount: finalizedAmount,
        real_remaining: realRemaining,
        original_amount: parseFloat(bill.amount),
      };
    });

    // 5. Validate allocations
    let totalAllocated = 0;
    const validationErrors = [];

    for (const [billId, allocatedAmount] of Object.entries(allocations)) {
      const allocatedNum = parseFloat(allocatedAmount) || 0;

      if (allocatedNum > 0) {
        const billDetail = billDetails.find((b) => b.id == billId);

        if (!billDetail) {
          validationErrors.push(`Bill ${billId} not found in selected bills`);
          continue;
        }

        const availableBalance = billDetail.real_remaining;

        if (allocatedNum > availableBalance + 0.01) {
          validationErrors.push({
            bill_id: billId,
            allocated: allocatedNum,
            available: availableBalance,
            message: `Allocation (Ghc ${allocatedNum.toFixed(
              2,
            )}) exceeds real remaining balance (Ghc ${availableBalance.toFixed(
              2,
            )})`,
          });
        }

        totalAllocated += allocatedNum;
      }
    }

    if (validationErrors.length > 0) {
      await connection.rollback();
      return res.status(400).json({
        error: "Allocation validation failed",
        details: validationErrors,
      });
    }

    // 6. Validate total allocation matches payment amount
    const allocationDifference = Math.abs(totalAllocated - amount_paid);
    if (allocationDifference > 0.01) {
      await connection.rollback();
      return res.status(400).json({
        error: `Total allocated amount (Ghc ${totalAllocated.toFixed(
          2,
        )}) does not match payment amount (Ghc ${amount_paid.toFixed(2)})`,
      });
    }

    // 7. Create payment record
    const [paymentResult] = await connection.query(
      `INSERT INTO payments (student_id, amount_paid, payment_date, payment_method, reference_number, received_by, notes) 
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [
        student_id,
        amount_paid,
        payment_date,
        payment_method,
        reference_number,
        received_by,
        notes,
      ],
    );

    const paymentId = paymentResult.insertId;

    // 8. Update individual bills
    for (const [billId, allocatedAmount] of Object.entries(allocations)) {
      const allocatedNum = parseFloat(allocatedAmount) || 0;

      if (allocatedNum > 0) {
        const billDetail = billDetails.find((b) => b.id == billId);

        if (billDetail) {
          const description = bill_descriptions
            ? bill_descriptions[billId]
            : null;

          const currentPaid = parseFloat(billDetail.paid_amount || 0);
          const newPaidAmount = currentPaid + allocatedNum;
          const finalizedAmount = billDetail.finalized_amount;

          const remainingAfterPayment = Math.max(
            0,
            finalizedAmount - newPaidAmount,
          );
          let paymentStatus = "Partially Paid";

          if (remainingAfterPayment <= 0.01) {
            paymentStatus = "Paid";
          } else if (newPaidAmount <= 0) {
            paymentStatus = "Pending";
          }

          await connection.query(
            `UPDATE bills SET 
              paid_amount = ?,
              remaining_amount = ?,
              payment_status = ?
             WHERE id = ?`,
            [newPaidAmount, remainingAfterPayment, paymentStatus, billId],
          );

          await connection.query(
            `INSERT INTO payment_allocations (payment_id, bill_id, amount_allocated, description) 
             VALUES (?, ?, ?, ?)`,
            [paymentId, billId, allocatedNum, description],
          );
        }
      }
    }

    // 9. Update finalized bill balance
    const currentPaidTotal = parseFloat(finalizedBill.paid_amount || 0);
    const newPaidTotal = currentPaidTotal + amount_paid;
    const totalAmount = parseFloat(finalizedBill.total_amount);
    const newRemainingBalance = Math.max(0, totalAmount - newPaidTotal);
    const isFullyPaid = newRemainingBalance <= 0.01;

    await connection.query(
      `UPDATE student_term_bills SET 
        paid_amount = ?,
        remaining_balance = ?,
        is_fully_paid = ?,
        last_payment_date = ?
       WHERE id = ?`,
      [
        newPaidTotal,
        newRemainingBalance,
        isFullyPaid,
        payment_date,
        finalizedBill.id,
      ],
    );

    // 10. Generate receipt
    const receiptNumber = `RCP-${Date.now()}-${paymentId}`;

    const [receiptResult] = await connection.query(
      `INSERT INTO receipts (receipt_number, student_id, payment_id, issued_date, issued_by, academic_year_id, term_id, notes) 
       VALUES (?, ?, ?, CURDATE(), ?, ?, ?, ?)`,
      [
        receiptNumber,
        student_id,
        paymentId,
        received_by,
        academicYearId,
        termId,
        notes,
      ],
    );

    await connection.commit();

    // ============ SEND EMAIL AND SMS NOTIFICATION ============
    let emailStatus = {
      sent: false,
      message: "No email address available",
      recipient: null,
    };

    let smsStatus = {
      sent: false,
      message: "No phone number available",
      recipient: null,
    };

    try {
      // Get student details with parent email and phone
      const [studentDetails] = await connection.query(
        `SELECT 
           s.id,
           s.first_name, 
           s.last_name, 
           s.admission_number,
           s.parent_name,
           s.parent_contact,
           s.parent_email,
           c.class_name
         FROM students s
         LEFT JOIN class_assignments ca ON s.id = ca.student_id AND ca.academic_year_id = ?
         LEFT JOIN classes c ON ca.class_id = c.id
         WHERE s.id = ?`,
        [academicYearId, student_id],
      );

      const student = studentDetails[0];

      if (student) {
        const emailService = require("../utils/emailServices");
        const smsService = require("../utils/smsService");

        // Send both email and SMS in background
        if (student.parent_email || student.student_email) {
          const recipientEmail = student.parent_email || student.student_email;
          emailStatus.recipient = recipientEmail;

          emailService
            .sendPaymentReceipt(
              {
                amount_paid,
                payment_method,
                payment_date,
                reference_number,
                payment_id: paymentId,
              },
              student,
              receiptNumber,
            )
            .then(async (result) => {
              if (result.success) {
                console.log(`✅ Email sent to ${recipientEmail}`);
                emailStatus.sent = true;
                emailStatus.message = "Email sent successfully";

                // Log to database
                try {
                  await pool.query(
                    `INSERT INTO email_logs 
                     (student_id, email_type, recipient_email, status, message_id, sent_at) 
                     VALUES (?, 'payment_receipt', ?, 'sent', ?, NOW())`,
                    [student.id, recipientEmail, result.messageId],
                  );
                } catch (logError) {
                  console.error("Error logging email:", logError);
                }
              } else {
                console.log(`⚠️ Email failed: ${result.message}`);
                emailStatus.sent = false;
                emailStatus.message = `Email failed: ${result.message}`;

                try {
                  await pool.query(
                    `INSERT INTO email_logs 
                     (student_id, email_type, recipient_email, status, error_message, sent_at) 
                     VALUES (?, 'payment_receipt', ?, 'failed', ?, NOW())`,
                    [student.id, recipientEmail, result.message],
                  );
                } catch (logError) {
                  console.error("Error logging failed email:", logError);
                }
              }
            })
            .catch((err) => {
              console.error("❌ Background email error:", err);
              emailStatus.sent = false;
              emailStatus.message = `Email error: ${err.message}`;
            });
        } else {
          emailStatus.message = "No email address on file";
          console.log(`📭 No email address for student ${student_id}`);
        }

        // Send SMS if phone number exists
        if (student.parent_contact) {
          const recipientPhone = student.parent_contact;
          smsStatus.recipient = recipientPhone;
          smsStatus.message = "Sending SMS...";

          smsService
            .sendPaymentReceiptSMS(
              student,
              {
                amount_paid,
                payment_method,
                payment_date,
                reference_number,
              },
              receiptNumber,
            )
            .then((result) => {
              if (result.success) {
                console.log(`✅ SMS sent to ${recipientPhone}`);
                smsStatus.sent = true;
                smsStatus.message = "SMS sent successfully";
                // Logging is already handled inside sendPaymentReceiptSMS
              } else {
                console.log(`⚠️ SMS failed: ${result.error}`);
                smsStatus.sent = false;
                smsStatus.message = `SMS failed: ${result.error}`;
                // Logging is already handled inside sendPaymentReceiptSMS
              }
            })
            .catch((err) => {
              console.error("❌ Background SMS error:", err);
              smsStatus.sent = false;
              smsStatus.message = `SMS error: ${err.message}`;
              // Logging is already handled inside sendPaymentReceiptSMS's catch
            });
        } else {
          smsStatus.message = "No phone number on file";
          console.log(`📭 No phone number for student ${student_id}`);
        }
      }
    } catch (notificationError) {
      console.error("Error preparing notifications:", notificationError);
      emailStatus.message = `Error: ${notificationError.message}`;
      smsStatus.message = `Error: ${notificationError.message}`;
    }

    // Clear caches
    clearRelevantCaches("PROCESS_PAYMENT", {
      student_id: student_id,
      academic_year_id: academicYearId,
      term_id: termId,
    });

    // 11. Get updated bill status for response
    const [updatedBills] = await connection.query(
      `SELECT id, description, amount, paid_amount, remaining_amount, payment_status 
       FROM bills WHERE id IN (?)`,
      [Object.keys(allocations).filter((id) => allocations[id] > 0)],
    );

    // ============ SEND RESPONSE ============
    res.json({
      success: true,
      message: `Payment of Ghc ${amount_paid.toFixed(
        2,
      )} processed successfully.`,
      receipt: {
        receipt_number: receiptNumber,
        payment_id: paymentId,
        amount: amount_paid,
      },
      balance: {
        previous_balance: parseFloat(
          finalizedBill.remaining_balance || totalAmount,
        ),
        paid: amount_paid,
        new_balance: newRemainingBalance,
        is_fully_paid: isFullyPaid,
      },
      bill_updates: updatedBills,
      term_bill: {
        id: finalizedBill.id,
        total_amount: totalAmount,
        new_paid_amount: newPaidTotal,
        new_remaining_balance: newRemainingBalance,
      },
      email: emailStatus,
      sms: smsStatus,
    });
  } catch (error) {
    await connection.rollback();
    clearRelevantCaches("PROCESS_PAYMENT");
    console.error("Error processing payment:", error);
    res.status(500).json({
      error: "Failed to process payment: " + error.message,
      stack: process.env.NODE_ENV === "development" ? error.stack : undefined,
    });
  } finally {
    connection.release();
  }
};

const generateReceiptPDF = async (req, res) => {
  try {
    const { receipt_number } = req.params;

    // Get receipt with all details including allocation breakdown WITH DESCRIPTIONS
    const [receipts] = await pool.query(
      `
      SELECT 
        r.*, 
        s.first_name, 
        s.last_name, 
        s.admission_number,
        s.parent_name,
        s.parent_contact,
        c.class_name,
        ay.year_label as academic_year,
        t.term_name,
        p.amount_paid, 
        p.payment_method, 
        p.payment_date,
        p.reference_number,
        p.notes as payment_notes,
        u.username as issued_by_name,
        -- Get allocation details WITH DESCRIPTIONS
        (SELECT JSON_ARRAYAGG(
          JSON_OBJECT(
            'category_name', fc.category_name,
            'description', COALESCE(pa.description, bt.description, fc.category_name), -- ✅ Use allocation description first
            'amount_allocated', pa.amount_allocated,
            'bill_description', bt.description
          )
        ) FROM payment_allocations pa
        JOIN bills b ON pa.bill_id = b.id
        JOIN bill_templates bt ON b.bill_template_id = bt.id
        JOIN fee_categories fc ON bt.fee_category_id = fc.id
        WHERE pa.payment_id = r.payment_id) as allocation_details
      FROM receipts r
      JOIN students s ON r.student_id = s.id
      JOIN payments p ON r.payment_id = p.id
      LEFT JOIN class_assignments ca ON s.id = ca.student_id AND ca.academic_year_id = r.academic_year_id
      LEFT JOIN classes c ON ca.class_id = c.id
      JOIN academic_years ay ON r.academic_year_id = ay.id
      JOIN terms t ON r.term_id = t.id
      JOIN users u ON r.issued_by = u.id
      WHERE r.receipt_number = ?
    `,
      [receipt_number],
    );

    if (receipts.length === 0) {
      return res.status(404).json({ error: "Receipt not found" });
    }

    const receipt = receipts[0];

    // Parse allocation details if they exist
    if (
      receipt.allocation_details &&
      typeof receipt.allocation_details === "string"
    ) {
      receipt.allocation_details = JSON.parse(receipt.allocation_details);
    }

    // Generate PDF
    const pdfBuffer = await generateReceiptPDFBuffer(receipt);

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader(
      "Content-Disposition",
      `inline; filename="receipt-${receipt_number}.pdf"`,
    );
    res.send(pdfBuffer);
  } catch (error) {
    console.error("Error generating receipt:", error);
    res.status(500).json({ error: "Failed to generate receipt" });
  }
};

//generate receipt PDF buffer
const generateReceiptPDFBuffer = async (receipt) => {
  const { jsPDF } = require("jspdf");
  const { autoTable } = require("jspdf-autotable");

  const doc = new jsPDF();
  const pageWidth = doc.internal.pageSize.getWidth();
  const pageHeight = doc.internal.pageSize.getHeight();

  // Get school settings
  const schoolSettings = await getSchoolSettingsForPDF();
  const [r, g, b] = [41, 128, 185]; // Primary color

  // ==================== GET TERM BILL FOR BALANCE INFO ====================
  let remainingBalance = null;
  let termBillInfo = null;

  try {
    const cacheKey = `term_balance_${receipt.student_id}_${receipt.academic_year_id}_${receipt.term_id}`;

    // CHECK CACHE FIRST
    const cachedBalance = cache.get(cacheKey);
    if (cachedBalance) {
      console.log(`📦 [CACHE HIT] Balance for student ${receipt.student_id}`);
      termBillInfo = cachedBalance;
      remainingBalance = parseFloat(cachedBalance.remaining_balance) || 0;
    } else {
      console.log(
        `🔄 [CACHE MISS] Fetching balance for student ${receipt.student_id}`,
      );

      // Get the term bill for this student/academic year/term
      const [termBill] = await pool.query(
        `SELECT remaining_balance, total_amount, is_fully_paid 
         FROM student_term_bills 
         WHERE student_id = ? 
           AND academic_year_id = ? 
           AND term_id = ? 
           AND is_finalized = TRUE`,
        [receipt.student_id, receipt.academic_year_id, receipt.term_id],
      );

      if (termBill.length > 0) {
        termBillInfo = termBill[0];
        remainingBalance = parseFloat(termBillInfo.remaining_balance) || 0;

        // CACHE FOR 30 SECONDS (balances change with payments)
        cache.set(cacheKey, termBillInfo, 30);
      }
    }
  } catch (error) {
    console.error("Error fetching term bill balance:", error);
  }

  // ==================== COMPACT HEADER ====================
  const hasLogo = await addSchoolLogoToPDF(doc, 15, 12, 15, 15);
  const schoolNameX = hasLogo ? 32 : 20;

  // School name
  doc.setFontSize(12);
  doc.setFont("helvetica", "bold");
  doc.setTextColor(r, g, b);
  doc.text(schoolSettings.school_name, schoolNameX, 18);

  // School motto
  if (schoolSettings.motto) {
    doc.setFontSize(8);
    doc.setFont("helvetica", "italic");
    doc.setTextColor(100, 100, 100);
    doc.text(schoolSettings.motto, schoolNameX, 23);
  }

  // Contact info on right side
  doc.setFontSize(8);
  doc.setFont("helvetica", "normal");
  doc.setTextColor(0, 0, 0);

  const rightStartX = pageWidth - 75;
  let rightY = 16;

  // Phone
  if (schoolSettings.phone_numbers && schoolSettings.phone_numbers.length > 0) {
    doc.text(`Phone: ${schoolSettings.phone_numbers[0]}`, rightStartX, rightY);
    rightY += 3;
  }

  // Email
  if (schoolSettings.email) {
    doc.text(`Email: ${schoolSettings.email}`, rightStartX, rightY);
    rightY += 3;
  }

  // Website
  if (schoolSettings.website) {
    doc.text(`Web: ${schoolSettings.website}`, rightStartX, rightY);
  }

  // Top divider line
  doc.setDrawColor(200, 200, 200);
  doc.setLineWidth(0.3);
  doc.line(15, 28, pageWidth - 15, 28);

  // ==================== RECEIPT TITLE ====================
  let yPosition = 34;
  doc.setFontSize(14);
  doc.setFont("helvetica", "bold");
  doc.setTextColor(r, g, b);
  doc.text("OFFICIAL RECEIPT", pageWidth / 2, yPosition, { align: "center" });
  yPosition += 5;

  // Receipt Number and Date
  doc.setFontSize(8);
  doc.setTextColor(0, 0, 0);
  doc.setFont("helvetica", "bold");
  doc.text(`Receipt: ${receipt.receipt_number}`, 20, yPosition);
  doc.text(
    `Date: ${new Date(receipt.issued_date).toLocaleDateString()}`,
    pageWidth - 20,
    yPosition,
    {
      align: "right",
    },
  );
  yPosition += 7;

  // ==================== STUDENT & PAYMENT INFO ====================
  doc.setDrawColor(200, 200, 200);
  doc.setFillColor(248, 248, 248);
  doc.roundedRect(15, yPosition, pageWidth - 30, 25, 2, 2, "FD");

  const col1X = 20;
  const col2X = pageWidth / 2 - 15;
  const col3X = pageWidth - 55;
  const boxY = yPosition + 6;

  // Column 1: Student Info
  doc.setFont("helvetica", "bold");
  doc.setFontSize(8);
  doc.text("STUDENT INFO", col1X, boxY - 1);

  doc.setFont("helvetica", "normal");
  doc.setFontSize(8);
  doc.text(`${receipt.first_name} ${receipt.last_name}`, col1X, boxY + 4);
  doc.text(`Adm: ${receipt.admission_number}`, col1X, boxY + 8);
  doc.text(`Class: ${receipt.class_name || "N/A"}`, col1X, boxY + 12);
  if (receipt.parent_name) {
    doc.text(
      `Parent: ${receipt.parent_name.substring(0, 15)}${
        receipt.parent_name.length > 15 ? "..." : ""
      }`,
      col1X,
      boxY + 16,
    );
  }

  // Column 2: Academic Info
  doc.setFont("helvetica", "bold");
  doc.setFontSize(8);
  doc.text("ACADEMIC INFO", col2X, boxY - 1);

  doc.setFont("helvetica", "normal");
  doc.setFontSize(7);
  doc.text(`Year: ${receipt.academic_year}`, col2X, boxY + 4);
  doc.text(`Term: ${receipt.term_name}`, col2X, boxY + 8);
  if (schoolSettings.principal_name) {
    const principalName = schoolSettings.principal_name || "";
    doc.text(
      `Principal: ${principalName.substring(0, 12)}${
        principalName.length > 12 ? "..." : ""
      }`,
      col2X,
      boxY + 12,
    );
  }

  // Column 3: Payment Details
  doc.setFont("helvetica", "bold");
  doc.setFontSize(8);
  doc.text("PAYMENT INFO", col3X, boxY - 1);

  doc.setFont("helvetica", "normal");
  doc.setFontSize(7);
  doc.text(
    `Amt: Ghc ${parseFloat(receipt.amount_paid).toFixed(2)}`,
    col3X,
    boxY + 4,
  );
  doc.text(`Method: ${receipt.payment_method}`, col3X, boxY + 8);
  doc.text(
    `Ref: ${receipt.reference_number?.substring(0, 8) || "N/A"}`,
    col3X,
    boxY + 12,
  );
  doc.text(
    `${new Date(receipt.payment_date).toLocaleDateString()}`,
    col3X,
    boxY + 16,
  );

  yPosition += 38;

  // ==================== FEE BREAKDOWN ====================
  if (receipt.allocation_details && receipt.allocation_details.length > 0) {
    doc.setFont("helvetica", "bold");
    doc.setFontSize(9);
    doc.text("FEE BREAKDOWN", 20, yPosition);
    yPosition += 6;

    const tableData = receipt.allocation_details.map((allocation) => [
      allocation.category_name || "Fee",
      allocation.description || allocation.bill_description || "Payment",
      `Ghc ${parseFloat(allocation.amount_allocated || 0).toFixed(2)}`,
    ]);

    autoTable(doc, {
      startY: yPosition,
      head: [["Category", "Description", "Amount"]],
      body: tableData,
      headStyles: {
        fillColor: [r, g, b],
        textColor: [255, 255, 255],
        fontStyle: "bold",
        fontSize: 8,
      },
      styles: {
        fontSize: 8,
        cellPadding: 1.5,
        lineColor: [200, 200, 200],
        lineWidth: 0.1,
      },
      columnStyles: {
        0: { cellWidth: 28 },
        1: { cellWidth: "auto" },
        2: { cellWidth: 23, halign: "right" },
      },
      margin: { left: 15, right: 15 },
      didDrawCell: (data) => {
        if (data.section === "body" && data.column.index === 1) {
          const lines = doc.splitTextToSize(data.cell.raw, data.cell.width - 1);
          if (lines.length > 1) {
            doc.text(lines, data.cell.x + 0.5, data.cell.y + 2.5);
            return false;
          }
        }
      },
    });

    yPosition = doc.lastAutoTable.finalY + 4;
  }

  // ==================== TOTAL AMOUNT BOX ====================
  doc.setFillColor(r, g, b);
  doc.rect(15, yPosition, pageWidth - 30, 7, "F");

  doc.setFontSize(10);
  doc.setFont("helvetica", "bold");
  doc.setTextColor(255, 255, 255);
  doc.text("TOTAL PAID:", 20, yPosition + 4.5);
  doc.text(
    `Ghc ${parseFloat(receipt.amount_paid).toFixed(2)}`,
    pageWidth - 20,
    yPosition + 4.5,
    {
      align: "right",
    },
  );

  yPosition += 13;

  // Remaining amount details box below TOTAL PAID
  (() => {
    // Safely compute numbers
    const rem =
      typeof remainingBalance === "number"
        ? remainingBalance
        : parseFloat(receipt.remaining_balance || 0) || 0;
    const termTotal =
      (termBillInfo && parseFloat(termBillInfo.total_amount)) ||
      parseFloat(receipt.total_amount || 0) ||
      0;
    const paidToDate =
      (termBillInfo && parseFloat(termBillInfo.paid_amount)) ||
      parseFloat(receipt.amount_paid || 0) ||
      0;

    // Light background box
    doc.setFillColor(248, 248, 248);
    doc.roundedRect(15, yPosition, pageWidth - 30, 14, 2, 2, "F");

    // Left column: Term total & Paid to date
    doc.setFontSize(9);
    doc.setFont("helvetica", "normal");
    doc.setTextColor(0, 0, 0);
    doc.text("Term Total:", 20, yPosition + 5.5);
    doc.setFont("helvetica", "bold");
    doc.text(`Ghc ${termTotal.toFixed(2)}`, 80, yPosition + 5.5);

    // Right column: Remaining balance emphasized
    doc.setFontSize(10);
    doc.setFont("helvetica", "normal");
    doc.text("Remaining Balance:", pageWidth - 90, yPosition + 6, {
      align: "left",
    });
    doc.setFont("helvetica", "bold");
    doc.setTextColor(41, 128, 185); // primary color
    doc.text(`Ghc ${rem.toFixed(2)}`, pageWidth - 20, yPosition + 6, {
      align: "right",
    });

    // Advance yPosition under this block for subsequent sections
    yPosition += 20;
  })();

  // ==================== NOTES ====================
  if (receipt.payment_notes) {
    doc.setFontSize(7);
    doc.setTextColor(0, 0, 0);
    doc.setFont("helvetica", "normal");

    // Notes label
    doc.setFont("helvetica", "bold");
    doc.text("Notes:", 20, yPosition);

    // Notes content
    doc.setFont("helvetica", "normal");
    const notesLines = doc.splitTextToSize(
      receipt.payment_notes,
      pageWidth - 40,
    );
    doc.text(notesLines, 28, yPosition + 3.5);
    yPosition += notesLines.length * 2.5 + 6;
  }

  // ==================== FOOTER ====================
  const footerY = pageHeight - 12;

  // Bottom divider
  doc.setDrawColor(200, 200, 200);
  doc.setLineWidth(0.3);
  doc.line(15, footerY - 8, pageWidth - 15, footerY - 8);

  // Footer text
  doc.setFontSize(6);
  doc.setTextColor(100, 100, 100);

  // Left: Issued by
  doc.text(`By: ${receipt.issued_by_name}`, 20, footerY - 3);

  // Center: Thank you message - change if fully paid
  if (remainingBalance !== null && remainingBalance <= 0.01) {
    doc.text("Fully Paid - Thank you!", pageWidth / 2, footerY - 3, {
      align: "center",
    });
  } else {
    doc.text("Thank you!", pageWidth / 2, footerY - 3, {
      align: "center",
    });
  }

  // Right: School name
  const schoolName =
    schoolSettings.school_short_name || schoolSettings.school_name;
  const displayName =
    schoolName.length > 20 ? schoolName.substring(0, 17) + "..." : schoolName;
  doc.text(displayName, pageWidth - 20, footerY - 3, {
    align: "right",
  });

  return Buffer.from(doc.output("arraybuffer"));
};

const getPaymentHistory = async (req, res) => {
  try {
    const { studentId } = req.params;
    const { academic_year_id, term_id } = req.query;

    let query = `
      SELECT 
        p.*,
        r.receipt_number,
        u.username as received_by_name,
        (SELECT COUNT(*) FROM payment_allocations pa WHERE pa.payment_id = p.id) as allocation_count
      FROM payments p
      LEFT JOIN receipts r ON p.id = r.payment_id
      LEFT JOIN users u ON p.received_by = u.id
      WHERE p.student_id = ?
    `;

    const queryParams = [studentId];

    // Add term filtering if provided
    if (academic_year_id && term_id) {
      query += ` AND r.academic_year_id = ? AND r.term_id = ?`;
      queryParams.push(academic_year_id, term_id);
    }

    query += ` ORDER BY p.payment_date DESC, p.id DESC`;

    const [payments] = await pool.query(query, queryParams);

    res.json(payments);
  } catch (error) {
    console.error("Error fetching payment history:", error);
    res.status(500).json({ error: "Failed to fetch payment history" });
  }
};

// GET /api/payment-allocations/:paymentId - Get allocation details for a payment
const getPaymentAllocations = async (req, res) => {
  try {
    const { paymentId } = req.params;

    const [allocations] = await pool.query(
      `
      SELECT 
        pa.*,
        b.amount as bill_amount,
        b.description as bill_description,
        b.due_date,
        b.status as bill_status,
        bt.is_compulsory,
        fc.category_name
      FROM payment_allocations pa
      LEFT JOIN bills b ON pa.bill_id = b.id
      LEFT JOIN bill_templates bt ON b.bill_template_id = bt.id
      LEFT JOIN fee_categories fc ON bt.fee_category_id = fc.id
      WHERE pa.payment_id = ?
      ORDER BY bt.is_compulsory DESC, fc.category_name
    `,
      [paymentId],
    );

    res.json(allocations);
  } catch (error) {
    console.error("Error fetching payment allocations:", error);
    res.status(500).json({ error: "Failed to fetch payment allocations" });
  }
};

//// GET /api/receipts - Get all receipts with filters
const getAllReceipts = async (req, res) => {
  try {
    const {
      receipt_number,
      student_name,
      admission_number,
      start_date,
      end_date,
      payment_method,
      academic_year_id,
      term_id,
      page = 1,
      limit = 20,
    } = req.query;

    let whereConditions = ["1=1"];
    let queryParams = [];
    let offset = (page - 1) * limit;

    if (receipt_number) {
      whereConditions.push("r.receipt_number LIKE ?");
      queryParams.push(`%${receipt_number}%`);
    }

    if (student_name) {
      whereConditions.push("(s.first_name LIKE ? OR s.last_name LIKE ?)");
      queryParams.push(`%${student_name}%`, `%${student_name}%`);
    }

    if (admission_number) {
      whereConditions.push("s.admission_number LIKE ?");
      queryParams.push(`%${admission_number}%`);
    }

    if (start_date && end_date) {
      whereConditions.push("r.issued_date BETWEEN ? AND ?");
      queryParams.push(start_date, end_date);
    }

    if (payment_method) {
      whereConditions.push("p.payment_method = ?");
      queryParams.push(payment_method);
    }

    if (academic_year_id) {
      whereConditions.push("r.academic_year_id = ?");
      queryParams.push(academic_year_id);
    }

    if (term_id) {
      whereConditions.push("r.term_id = ?");
      queryParams.push(term_id);
    }

    // Get receipts with pagination
    const [receipts] = await pool.query(
      `SELECT 
        r.*,
        s.first_name,
        s.last_name,
        s.admission_number,
        s.parent_name,
        s.parent_contact,
        c.class_name,
        ay.year_label as academic_year,
        t.term_name,
        p.amount_paid,
        p.payment_method,
        p.payment_date,
        p.reference_number,
        u.username as issued_by_name,
        (SELECT COUNT(*) FROM payment_allocations pa WHERE pa.payment_id = r.payment_id) as allocation_count
       FROM receipts r
       JOIN students s ON r.student_id = s.id
       JOIN payments p ON r.payment_id = p.id
       LEFT JOIN class_assignments ca ON s.id = ca.student_id AND r.academic_year_id = ca.academic_year_id
       LEFT JOIN classes c ON ca.class_id = c.id
       JOIN academic_years ay ON r.academic_year_id = ay.id
       JOIN terms t ON r.term_id = t.id
       JOIN users u ON r.issued_by = u.id
       WHERE ${whereConditions.join(" AND ")}
       ORDER BY r.issued_date DESC, r.id DESC
       LIMIT ? OFFSET ?`,
      [...queryParams, parseInt(limit), offset],
    );

    // Get total count for pagination
    const [countResult] = await pool.query(
      `SELECT COUNT(*) as total 
       FROM receipts r
       JOIN students s ON r.student_id = s.id
       JOIN payments p ON r.payment_id = p.id
       WHERE ${whereConditions.join(" AND ")}`,
      queryParams,
    );

    res.json({
      receipts,
      pagination: {
        page: parseInt(page),
        limit: parseInt(limit),
        total: countResult[0].total,
        pages: Math.ceil(countResult[0].total / limit),
      },
    });
  } catch (error) {
    console.error("Error fetching receipts:", error);
    res.status(500).json({ error: "Failed to fetch receipts" });
  }
};

//financial records
// GET /api/financial-records/payments-by-category - Get payments by category with filters
const getPaymentsByCategory = async (req, res) => {
  try {
    const {
      fee_category_id,
      academic_year_id,
      term_id,
      start_date,
      end_date,
      class_id,
      payment_method,
      page = 1,
      limit = 50,
    } = req.query;

    let whereConditions = ["1=1"];
    let queryParams = [];
    let offset = (page - 1) * limit;

    // Required filters
    if (fee_category_id) {
      whereConditions.push("fc.id = ?");
      queryParams.push(fee_category_id);
    }

    if (academic_year_id) {
      whereConditions.push("r.academic_year_id = ?");
      queryParams.push(academic_year_id);
    }

    if (term_id) {
      whereConditions.push("r.term_id = ?");
      queryParams.push(term_id);
    }

    // Date range
    if (start_date && end_date) {
      whereConditions.push("p.payment_date BETWEEN ? AND ?");
      queryParams.push(start_date, end_date);
    } else if (start_date) {
      whereConditions.push("p.payment_date >= ?");
      queryParams.push(start_date);
    } else if (end_date) {
      whereConditions.push("p.payment_date <= ?");
      queryParams.push(end_date);
    }

    // Optional filters
    if (class_id) {
      whereConditions.push("c.id = ?");
      queryParams.push(class_id);
    }

    if (payment_method) {
      whereConditions.push("p.payment_method = ?");
      queryParams.push(payment_method);
    }

    // Get payments with detailed information
    const [payments] = await pool.query(
      `
      SELECT 
        p.id as payment_id,
        p.amount_paid,
        p.payment_date,
        p.payment_method,
        p.reference_number,
        p.notes as payment_notes,
        pa.amount_allocated,
        pa.description as allocation_description,
        r.receipt_number,
        r.issued_date,
        
        -- Student Information
        s.id as student_id,
        s.first_name,
        s.last_name,
        s.admission_number,
        
        -- Class Information
        c.id as class_id,
        c.class_name,
        
        -- Fee Category Information
        fc.id as fee_category_id,
        fc.category_name,
        fc.description as category_description,
        
        -- Academic Information
        ay.year_label as academic_year,
        t.term_name,
        
        -- Received by
        u.username as received_by_name

      FROM payments p
      
      -- Join with payment allocations to get category information
      INNER JOIN payment_allocations pa ON p.id = pa.payment_id
      INNER JOIN bills b ON pa.bill_id = b.id
      INNER JOIN bill_templates bt ON b.bill_template_id = bt.id
      INNER JOIN fee_categories fc ON bt.fee_category_id = fc.id
      
      -- Join with receipts for academic context
      INNER JOIN receipts r ON p.id = r.payment_id
      
      -- Student and class information
      INNER JOIN students s ON p.student_id = s.id
      LEFT JOIN class_assignments ca ON s.id = ca.student_id AND r.academic_year_id = ca.academic_year_id
      LEFT JOIN classes c ON ca.class_id = c.id
      
      -- Academic context
      INNER JOIN academic_years ay ON r.academic_year_id = ay.id
      INNER JOIN terms t ON r.term_id = t.id
      
      -- User information
      INNER JOIN users u ON p.received_by = u.id
      
      WHERE ${whereConditions.join(" AND ")}
      ORDER BY p.payment_date DESC, p.id DESC
      LIMIT ? OFFSET ?
      `,
      [...queryParams, parseInt(limit), offset],
    );

    // Get total count for pagination - FIXED QUERY
    const [countResult] = await pool.query(
      `
      SELECT COUNT(DISTINCT p.id) as total
      FROM payments p
      INNER JOIN payment_allocations pa ON p.id = pa.payment_id
      INNER JOIN bills b ON pa.bill_id = b.id
      INNER JOIN bill_templates bt ON b.bill_template_id = bt.id
      INNER JOIN fee_categories fc ON bt.fee_category_id = fc.id
      INNER JOIN receipts r ON p.id = r.payment_id
      INNER JOIN students s ON p.student_id = s.id
      LEFT JOIN class_assignments ca ON s.id = ca.student_id AND r.academic_year_id = ca.academic_year_id
      LEFT JOIN classes c ON ca.class_id = c.id
      WHERE ${whereConditions.join(" AND ")}
      `,
      queryParams,
    );

    // Get summary statistics - FIXED QUERY
    const [summary] = await pool.query(
      `
      SELECT 
        COUNT(DISTINCT p.id) as total_payments,
        COUNT(DISTINCT s.id) as total_students,
        SUM(pa.amount_allocated) as total_amount,
        AVG(pa.amount_allocated) as average_payment,
        fc.category_name
      FROM payments p
      INNER JOIN payment_allocations pa ON p.id = pa.payment_id
      INNER JOIN bills b ON pa.bill_id = b.id
      INNER JOIN bill_templates bt ON b.bill_template_id = bt.id
      INNER JOIN fee_categories fc ON bt.fee_category_id = fc.id
      INNER JOIN receipts r ON p.id = r.payment_id
      INNER JOIN students s ON p.student_id = s.id
      LEFT JOIN class_assignments ca ON s.id = ca.student_id AND r.academic_year_id = ca.academic_year_id
      LEFT JOIN classes c ON ca.class_id = c.id
      WHERE ${whereConditions.join(" AND ")}
      GROUP BY fc.id, fc.category_name
      `,
      queryParams,
    );

    res.json({
      payments,
      summary: summary[0] || {},
      pagination: {
        page: parseInt(page),
        limit: parseInt(limit),
        total: countResult[0].total,
        pages: Math.ceil(countResult[0].total / limit),
      },
      filters: {
        fee_category_id,
        academic_year_id,
        term_id,
        start_date,
        end_date,
        class_id,
        payment_method,
      },
    });
  } catch (error) {
    console.error("Error fetching payments by category:", error);
    res.status(500).json({ error: "Failed to fetch payment records" });
  }
};

// GET /api/financial-records/student-statements - Get student statements
const getStudentStatements = async (req, res) => {
  try {
    const {
      student_id,
      admission_number,
      class_id,
      academic_year_id,
      term_id,
      status,
      student_name,
      page = 1,
      limit = 50,
    } = req.query;

    let whereConditions = ["1=1"];
    let queryParams = [];
    let offset = (page - 1) * limit;

    if (student_id) {
      whereConditions.push("s.id = ?");
      queryParams.push(student_id);
    }

    if (admission_number) {
      whereConditions.push("s.admission_number = ?");
      queryParams.push(admission_number);
    }

    if (student_name) {
      whereConditions.push(
        "(s.first_name LIKE ? OR s.last_name LIKE ? OR CONCAT(s.first_name, ' ', s.last_name) LIKE ?)",
      );
      queryParams.push(
        `%${student_name}%`,
        `%${student_name}%`,
        `%${student_name}%`,
      );
    }

    if (class_id) {
      whereConditions.push("c.id = ?");
      queryParams.push(class_id);
    }

    if (academic_year_id) {
      whereConditions.push("stb.academic_year_id = ?");
      queryParams.push(academic_year_id);
    }

    if (term_id) {
      whereConditions.push("stb.term_id = ?");
      queryParams.push(term_id);
    }

    // Status filter
    if (status === "owing") {
      whereConditions.push(
        "(stb.remaining_balance > 0 OR stb.remaining_balance IS NULL)",
      );
    } else if (status === "fully_paid") {
      whereConditions.push("stb.remaining_balance <= 0");
    } else if (status === "pending") {
      whereConditions.push("(stb.total_amount > 0 AND stb.paid_amount = 0)");
    }

    // Get current academic year ID if not provided
    let currentAcademicYearId = null;
    if (!academic_year_id) {
      const [currentYear] = await pool.query(
        "SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1",
      );
      currentAcademicYearId = currentYear[0]?.id;
    }

    // Get current term ID if not provided
    let currentTermId = null;
    if (!term_id) {
      const [currentTerm] = await pool.query(
        "SELECT id FROM terms ORDER BY start_date DESC LIMIT 1",
      );
      currentTermId = currentTerm[0]?.id;
    }

    const [students] = await pool.query(
      `
      SELECT 
        s.id as student_id,
        s.admission_number,
        s.first_name,
        s.last_name,
        s.parent_name,
        s.parent_contact,
        s.parent_email,
        c.class_name,
        
        -- Term bill information
        stb.id as term_bill_id,
        stb.total_amount,
        stb.paid_amount,
        stb.remaining_balance,
        stb.is_fully_paid,
        stb.is_finalized,
        stb.last_payment_date,
        
        -- Academic context
        ay.year_label as academic_year,
        t.term_name,
        
        -- Payment summary
        COALESCE(payment_summary.total_payments, 0) as payment_count,
        COALESCE(payment_summary.total_paid, 0) as total_paid_to_date,
        COALESCE(payment_summary.last_payment_date, NULL) as last_payment
        
      FROM students s
      
      -- Class information
      LEFT JOIN class_assignments ca ON s.id = ca.student_id 
        AND ca.academic_year_id = COALESCE(?, ?)
      LEFT JOIN classes c ON ca.class_id = c.id
      
      -- Term bill (if finalized)
      LEFT JOIN student_term_bills stb ON s.id = stb.student_id
        AND stb.is_finalized = TRUE
        AND (stb.academic_year_id = COALESCE(?, ?) OR ? IS NULL)
        AND (stb.term_id = COALESCE(?, ?) OR ? IS NULL)
      
      -- Academic context
      LEFT JOIN academic_years ay ON stb.academic_year_id = ay.id
      LEFT JOIN terms t ON stb.term_id = t.id
      
      -- Payment history
      LEFT JOIN (
        SELECT 
          student_id,
          COUNT(*) as total_payments,
          SUM(amount_paid) as total_paid,
          MAX(payment_date) as last_payment_date
        FROM payments
        GROUP BY student_id
      ) payment_summary ON s.id = payment_summary.student_id
      
      WHERE ${whereConditions.join(" AND ")}
        AND (s.is_active IS NULL OR s.is_active = TRUE)
      
      ORDER BY c.class_name, s.first_name, s.last_name
      LIMIT ? OFFSET ?
      `,
      [
        academic_year_id,
        currentAcademicYearId,
        academic_year_id,
        currentAcademicYearId,
        academic_year_id,
        term_id,
        currentTermId,
        term_id,
        ...queryParams,
        parseInt(limit),
        offset,
      ],
    );

    // Get total count
    const [countResult] = await pool.query(
      `
      SELECT COUNT(DISTINCT s.id) as total
      FROM students s
      LEFT JOIN class_assignments ca ON s.id = ca.student_id 
        AND ca.academic_year_id = COALESCE(?, ?)
      LEFT JOIN classes c ON ca.class_id = c.id
      LEFT JOIN student_term_bills stb ON s.id = stb.student_id
        AND stb.is_finalized = TRUE
        AND (stb.academic_year_id = COALESCE(?, ?) OR ? IS NULL)
        AND (stb.term_id = COALESCE(?, ?) OR ? IS NULL)
      WHERE ${whereConditions.join(" AND ")}
        AND (s.is_active IS NULL OR s.is_active = TRUE)
      `,
      [
        academic_year_id,
        currentAcademicYearId,
        academic_year_id,
        currentAcademicYearId,
        academic_year_id,
        term_id,
        currentTermId,
        term_id,
        ...queryParams,
      ],
    );

    res.json({
      students,
      pagination: {
        page: parseInt(page),
        limit: parseInt(limit),
        total: countResult[0].total,
        pages: Math.ceil(countResult[0].total / limit),
      },
      summary: {
        total_students: countResult[0].total,
        total_owing: students.filter((s) => s.remaining_balance > 0).length,
        total_paid: students.filter(
          (s) => s.remaining_balance <= 0 && s.remaining_balance !== null,
        ).length,
        total_pending: students.filter((s) => !s.is_finalized).length,
      },
    });
  } catch (error) {
    console.error("Error fetching student statements:", error);
    res.status(500).json({ error: "Failed to fetch student statements" });
  }
};

// GET /api/financial-records/class-collections - Get class-wise collection summary
const getClassCollections = async (req, res) => {
  try {
    const { academic_year_id, term_id, start_date, end_date } = req.query;

    let whereConditions = ["1=1"];
    let queryParams = [];

    if (academic_year_id) {
      whereConditions.push("r.academic_year_id = ?");
      queryParams.push(academic_year_id);
    }

    if (term_id) {
      whereConditions.push("r.term_id = ?");
      queryParams.push(term_id);
    }

    if (start_date && end_date) {
      whereConditions.push("p.payment_date BETWEEN ? AND ?");
      queryParams.push(start_date, end_date);
    }

    // Get current academic year ID if not provided
    let currentAcademicYearId = null;
    if (!academic_year_id) {
      const [currentYear] = await pool.query(
        "SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1",
      );
      currentAcademicYearId = currentYear[0]?.id;
    }

    // Get current term ID if not provided
    let currentTermId = null;
    if (!term_id) {
      const [currentTerm] = await pool.query(
        "SELECT id FROM terms ORDER BY start_date DESC LIMIT 1",
      );
      currentTermId = currentTerm[0]?.id;
    }

    const [classCollections] = await pool.query(
      `
      SELECT 
        c.id as class_id,
        c.class_name,
        COUNT(DISTINCT s.id) as total_students,
        COUNT(DISTINCT p.id) as total_payments,
        SUM(pa.amount_allocated) as total_collected,
        AVG(pa.amount_allocated) as average_payment,
        
        -- Student bill status
        SUM(CASE WHEN stb.remaining_balance > 0 THEN 1 ELSE 0 END) as students_owing,
        SUM(CASE WHEN stb.remaining_balance <= 0 AND stb.remaining_balance IS NOT NULL THEN 1 ELSE 0 END) as students_fully_paid,
        SUM(CASE WHEN stb.is_finalized IS NULL OR stb.is_finalized = FALSE THEN 1 ELSE 0 END) as students_pending
        
      FROM classes c
      
      -- Get students in class for current academic year
      INNER JOIN class_assignments ca ON c.id = ca.class_id
        AND ca.academic_year_id = COALESCE(?, ?)
      INNER JOIN students s ON ca.student_id = s.id
      
      -- Get payments for these students
      LEFT JOIN payments p ON s.id = p.student_id
      LEFT JOIN payment_allocations pa ON p.id = pa.payment_id
      LEFT JOIN receipts r ON p.id = r.payment_id
        AND (r.academic_year_id = ca.academic_year_id OR r.academic_year_id IS NULL)
      
      -- Get term bills
      LEFT JOIN student_term_bills stb ON s.id = stb.student_id
        AND stb.academic_year_id = ca.academic_year_id
        AND stb.term_id = COALESCE(?, ?)
      
      WHERE ${whereConditions.join(" AND ")}
        AND (s.is_active IS NULL OR s.is_active = TRUE)
      
      GROUP BY c.id, c.class_name
      ORDER BY total_collected DESC, c.class_name
      `,
      [
        academic_year_id,
        currentAcademicYearId,
        term_id,
        currentTermId,
        ...queryParams,
      ],
    );

    res.json({
      classCollections,
      total_summary: {
        total_classes: classCollections.length,
        total_collected: classCollections.reduce(
          (sum, cls) => sum + parseFloat(cls.total_collected || 0),
          0,
        ),
        total_students: classCollections.reduce(
          (sum, cls) => sum + parseInt(cls.total_students || 0),
          0,
        ),
        total_payments: classCollections.reduce(
          (sum, cls) => sum + parseInt(cls.total_payments || 0),
          0,
        ),
      },
    });
  } catch (error) {
    console.error("Error fetching class collections:", error);
    res.status(500).json({ error: "Failed to fetch class collections" });
  }
};

const exportFinancialData = async (req, res) => {
  try {
    const {
      exportType,
      filters,
      format = "excel", // 'excel' or 'pdf'
    } = req.body;

    if (format === "pdf") {
      // Handle PDF export
      await handlePDFExport(res, exportType, filters);
    } else {
      // Handle Excel export (existing code)
      await handleExcelExport(res, exportType, filters);
    }
  } catch (error) {
    console.error("Error exporting financial data:", error);
    res.status(500).json({ error: "Failed to export data" });
  }
};

// Handle Excel export
const handleExcelExport = async (res, exportType, filters) => {
  let data = [];
  let fileName = "";
  let worksheetName = "";

  // Fetch data based on export type
  switch (exportType) {
    case "payments-by-category":
      data = await fetchPaymentsForExport(filters);
      fileName = `payments-export-${
        new Date().toISOString().split("T")[0]
      }.xlsx`;
      worksheetName = "Payments";
      break;

    case "student-statements":
      data = await fetchStudentStatementsForExport(filters);
      fileName = `student-statements-${
        new Date().toISOString().split("T")[0]
      }.xlsx`;
      worksheetName = "Student Statements";
      break;

    case "class-collections":
      data = await fetchClassCollectionsForExport(filters);
      fileName = `class-collections-${
        new Date().toISOString().split("T")[0]
      }.xlsx`;
      worksheetName = "Class Collections";
      break;

    default:
      return res.status(400).json({ error: "Invalid export type" });
  }

  // Create workbook
  const workbook = XLSX.utils.book_new();

  // Convert data to worksheet
  const worksheet = XLSX.utils.json_to_sheet(data);

  // Add worksheet to workbook
  XLSX.utils.book_append_sheet(workbook, worksheet, worksheetName);

  // Generate buffer
  const buffer = XLSX.write(workbook, { type: "buffer", bookType: "xlsx" });

  // Set headers
  res.setHeader(
    "Content-Type",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  );
  res.setHeader("Content-Disposition", `attachment; filename="${fileName}"`);

  res.send(buffer);
};

// Handle PDF export
const handlePDFExport = async (res, exportType, filters) => {
  let data = [];
  let headers = [];
  let fileName = "";
  let reportTitle = "";

  // Fetch data and set headers based on export type
  switch (exportType) {
    case "payments-by-category":
      data = await fetchPaymentsForExport(filters);
      headers = [
        [
          "Receipt No",
          "Student",
          "Class",
          "Category",
          "Description",
          "Amount",
          "Method",
          "Date",
          "Received By",
        ],
      ];
      fileName = `payments-export-${
        new Date().toISOString().split("T")[0]
      }.pdf`;
      reportTitle = "PAYMENTS BY CATEGORY REPORT";
      break;

    case "student-statements":
      data = await fetchStudentStatementsForExport(filters);
      headers = [
        [
          "Admission No",
          "Student Name",
          "Class",
          "Total Bill",
          "Paid",
          "Balance",
          "Status",
          "Last Payment",
        ],
      ];
      fileName = `student-statements-${
        new Date().toISOString().split("T")[0]
      }.pdf`;
      reportTitle = "STUDENT STATEMENTS REPORT";
      break;

    case "class-collections":
      data = await fetchClassCollectionsForExport(filters);
      headers = [
        [
          "Class",
          "Students",
          "Total Collected",
          "Avg Payment",
          "Payments",
          "Owing",
          "Fully Paid",
          "Pending",
          "Collection Rate",
        ],
      ];
      fileName = `class-collections-${
        new Date().toISOString().split("T")[0]
      }.pdf`;
      reportTitle = "CLASS COLLECTIONS REPORT";
      break;

    default:
      return res.status(400).json({ error: "Invalid export type" });
  }

  // Generate PDF
  const pdfBuffer = await generatePDF(
    reportTitle,
    headers,
    data,
    filters,
    exportType,
  );

  // Set headers
  res.setHeader("Content-Type", "application/pdf");
  res.setHeader("Content-Disposition", `attachment; filename="${fileName}"`);

  res.send(pdfBuffer);
};

// Generate PDF document
const generatePDF = async (title, headers, data, filters, exportType) => {
  // Create new PDF document
  const doc = new jsPDF("landscape"); // Landscape for better table fit
  const schoolSettings = await getSchoolSettingsForPDF();

  const pageWidth = doc.internal.pageSize.getWidth();
  const pageHeight = doc.internal.pageSize.getHeight();

  // Colors
  const primaryColor = [41, 128, 185];
  const secondaryColor = [52, 152, 219];

  // Header with school info
  doc.setFillColor(...primaryColor);
  doc.rect(0, 0, pageWidth, 30, "F");
  const hasLogo = await addSchoolLogoToPDF(doc, 15, 3, 25, 25);

  // School name
  doc.setTextColor(255, 255, 255);
  doc.setFontSize(20);
  doc.setFont("helvetica", "bold");
  doc.text(schoolSettings.school_name, hasLogo ? 90 : pageWidth / 2, 12, {
    align: hasLogo ? "left" : "center",
  });

  // Report title
  doc.setFontSize(16);
  doc.text(title, pageWidth / 2, 22, { align: "center" });

  // Filters information
  let startY = 40;
  doc.setFontSize(10);
  doc.setTextColor(0, 0, 0);
  doc.setFont("helvetica", "normal");

  // Add filter information if available
  const filterText = [];

  if (filters.academic_year_id) {
    const [year] = await pool.query(
      "SELECT year_label FROM academic_years WHERE id = ?",
      [filters.academic_year_id],
    );
    if (year.length > 0) {
      filterText.push(`Academic Year: ${year[0].year_label}`);
    }
  }

  if (filters.term_id) {
    const [term] = await pool.query(
      "SELECT term_name FROM terms WHERE id = ?",
      [filters.term_id],
    );
    if (term.length > 0) {
      filterText.push(`Term: ${term[0].term_name}`);
    }
  }

  if (filters.start_date && filters.end_date) {
    filterText.push(`Date Range: ${filters.start_date} to ${filters.end_date}`);
  }

  if (filters.fee_category_id) {
    const [category] = await pool.query(
      "SELECT category_name FROM fee_categories WHERE id = ?",
      [filters.fee_category_id],
    );
    if (category.length > 0) {
      filterText.push(`Fee Category: ${category[0].category_name}`);
    }
  }

  if (filters.class_id) {
    const [classInfo] = await pool.query(
      "SELECT class_name FROM classes WHERE id = ?",
      [filters.class_id],
    );
    if (classInfo.length > 0) {
      filterText.push(`Class: ${classInfo[0].class_name}`);
    }
  }

  if (filters.status) {
    const statusMap = {
      owing: "Students Owing",
      fully_paid: "Fully Paid",
      pending: "Pending Bills",
    };
    filterText.push(`Status: ${statusMap[filters.status] || filters.status}`);
  }

  // Add filters to PDF
  if (filterText.length > 0) {
    doc.setFont("helvetica", "bold");
    doc.text("Filters Applied:", 14, startY);
    doc.setFont("helvetica", "normal");

    filterText.forEach((text, index) => {
      doc.text(`• ${text}`, 20, startY + 5 + index * 5);
    });

    startY += filterText.length * 5 + 10;
  } else {
    startY += 10;
  }

  // Prepare table data
  const tableData = prepareTableData(data, exportType);

  // Generate table
  autoTable(doc, {
    startY: startY,
    head: headers,
    body: tableData,
    headStyles: {
      fillColor: primaryColor,
      textColor: [255, 255, 255],
      fontStyle: "bold",
      fontSize: 9,
    },
    bodyStyles: {
      fontSize: 8,
      cellPadding: 2,
    },
    alternateRowStyles: {
      fillColor: [248, 248, 248],
    },
    styles: {
      overflow: "linebreak",
      cellWidth: "wrap",
    },
    margin: { left: 10, right: 10 },
    didDrawPage: (data) => {
      // Add page numbers
      const pageCount = doc.internal.getNumberOfPages();
      doc.setFontSize(8);
      doc.setTextColor(100, 100, 100);
      doc.text(
        `Page ${data.pageNumber} of ${pageCount}`,
        pageWidth / 2,
        pageHeight - 10,
        { align: "center" },
      );
    },
  });

  // Add summary section if data exists
  const finalY = doc.lastAutoTable.finalY + 10;
  if (finalY < pageHeight - 50 && data.length > 0) {
    addSummarySection(doc, data, exportType, finalY, pageWidth);
  }
  // if (data.length > 0) {
  //   // Always add a new page for summary to ensure it's visible
  //   doc.addPage();

  //   // Start at top of new page
  //   const summaryStartY = 30;

  //   // Summary header
  //   doc.setFontSize(14);
  //   doc.setFont("helvetica", "bold");
  //   doc.setTextColor(41, 128, 185); // Blue color
  //   doc.text("SUMMARY REPORT", pageWidth / 2, summaryStartY, {
  //     align: "center",
  //   });

  //   // Add summary content
  //   addSummarySection(doc, data, exportType, summaryStartY + 10, pageWidth);
  // }
  // Add footer
  doc.setFontSize(8);
  doc.setTextColor(100, 100, 100);
  doc.text(
    `Generated on ${new Date().toLocaleDateString()} | ${
      schoolSettings.school_name
    }`,
    pageWidth / 2,
    pageHeight - 5,
    { align: "center" },
  );

  // Return PDF as buffer
  return Buffer.from(doc.output("arraybuffer"));
};

// Prepare table data for PDF
const prepareTableData = (data, exportType) => {
  switch (exportType) {
    case "payments-by-category":
      return data.map((item) => [
        item["Receipt Number"] || "",
        item["Student Name"] || "",
        item["Class"] || "",
        item["Fee Category"] || "",
        item["Description"] || "",
        formatCurrencyForPDF(item["Amount"] || 0),
        item["Payment Method"] || "",
        item["Payment Date"] || "",
        item["Received By"] || "",
      ]);

    case "student-statements":
      return data.map((item) => [
        item["Admission Number"] || "",
        item["Student Name"] || "",
        item["Class"] || "",
        formatCurrencyForPDF(item["Total Bill"] || 0),
        formatCurrencyForPDF(item["Amount Paid"] || 0),
        formatCurrencyForPDF(item["Balance"] || 0),
        item["Status"] || "",
        item["Last Payment Date"] || "No payments",
      ]);

    case "class-collections":
      return data.map((item) => [
        item["Class"] || "",
        item["Total Students"] || 0,
        formatCurrencyForPDF(item["Total Collected"] || 0),
        formatCurrencyForPDF(item["Average Payment"] || 0),
        item["Total Payments"] || 0,
        item["Students Owing"] || 0,
        item["Students Fully Paid"] || 0,
        item["Students Pending"] || 0,
        `${item["Collection Rate (%)"] || 0}%`,
      ]);

    default:
      return [];
  }
};

// Add summary section to PDF
const addSummarySection = (doc, data, exportType, startY, pageWidth) => {
  doc.setFontSize(10);
  doc.setFont("helvetica", "bold");
  doc.setTextColor(0, 0, 0);
  doc.text("SUMMARY", 14, startY);

  doc.setFont("helvetica", "normal");
  doc.setFontSize(9);

  let summaryY = startY + 8;

  switch (exportType) {
    case "payments-by-category":
      const totalAmount = data.reduce(
        (sum, item) => sum + (parseFloat(item["Amount"]) || 0),
        0,
      );
      const totalPayments = data.length;
      const uniqueStudents = [
        ...new Set(data.map((item) => item["Admission Number"])),
      ].length;

      doc.text(
        `Total Amount Collected: ${formatCurrencyForPDF(totalAmount)}`,
        20,
        summaryY,
      );
      summaryY += 5;
      doc.text(`Total Payments: ${totalPayments}`, 20, summaryY);
      summaryY += 5;
      doc.text(`Students Paid: ${uniqueStudents}`, 20, summaryY);
      break;

    case "student-statements":
      const totalBill = data.reduce(
        (sum, item) => sum + (parseFloat(item["Total Bill"]) || 0),
        0,
      );
      const totalPaid = data.reduce(
        (sum, item) => sum + (parseFloat(item["Amount Paid"]) || 0),
        0,
      );
      const totalBalance = data.reduce(
        (sum, item) => sum + (parseFloat(item["Balance"]) || 0),
        0,
      );
      const owingCount = data.filter(
        (item) => parseFloat(item["Balance"]) > 0,
      ).length;
      const paidCount = data.filter(
        (item) => parseFloat(item["Balance"]) <= 0 && item["Balance"] !== null,
      ).length;

      doc.text(
        `Total Bill Amount: ${formatCurrencyForPDF(totalBill)}`,
        20,
        summaryY,
      );
      summaryY += 5;
      doc.text(`Total Paid: ${formatCurrencyForPDF(totalPaid)}`, 20, summaryY);
      summaryY += 5;
      doc.text(
        `Total Outstanding: ${formatCurrencyForPDF(totalBalance)}`,
        20,
        summaryY,
      );
      summaryY += 5;
      doc.text(`Students Owing: ${owingCount}`, 20, summaryY);
      summaryY += 5;
      doc.text(`Students Fully Paid: ${paidCount}`, 20, summaryY);
      break;

    case "class-collections":
      const totalCollected = data.reduce(
        (sum, item) => sum + (parseFloat(item["Total Collected"]) || 0),
        0,
      );
      const totalStudents = data.reduce(
        (sum, item) => sum + (parseInt(item["Total Students"]) || 0),
        0,
      );
      const totalOwing = data.reduce(
        (sum, item) => sum + (parseInt(item["Students Owing"]) || 0),
        0,
      );
      const totalFullyPaid = data.reduce(
        (sum, item) => sum + (parseInt(item["Students Fully Paid"]) || 0),
        0,
      );
      const avgCollectionRate =
        data.reduce(
          (sum, item) => sum + (parseFloat(item["Collection Rate (%)"]) || 0),
          0,
        ) / data.length;

      doc.text(
        `Total Collected: ${formatCurrencyForPDF(totalCollected)}`,
        20,
        summaryY,
      );
      summaryY += 5;
      doc.text(`Total Students: ${totalStudents}`, 20, summaryY);
      summaryY += 5;
      doc.text(`Students Owing: ${totalOwing}`, 20, summaryY);
      summaryY += 5;
      doc.text(`Students Fully Paid: ${totalFullyPaid}`, 20, summaryY);
      summaryY += 5;
      doc.text(
        `Average Collection Rate: ${avgCollectionRate.toFixed(1)}%`,
        20,
        summaryY,
      );
      break;
  }
};

// Helper function to format currency for PDF
const formatCurrencyForPDF = (amount) => {
  const num = parseFloat(amount);
  return isNaN(num) ? "Ghc 0.00" : `Ghc ${num.toFixed(2)}`;
};

// Helper function to fetch payments for export
const fetchPaymentsForExport = async (filters) => {
  const {
    fee_category_id,
    academic_year_id,
    term_id,
    start_date,
    end_date,
    class_id,
    payment_method,
  } = filters;

  let whereConditions = ["1=1"];
  let queryParams = [];

  if (fee_category_id) {
    whereConditions.push("fc.id = ?");
    queryParams.push(fee_category_id);
  }

  if (academic_year_id) {
    whereConditions.push("r.academic_year_id = ?");
    queryParams.push(academic_year_id);
  }

  if (term_id) {
    whereConditions.push("r.term_id = ?");
    queryParams.push(term_id);
  }

  if (start_date && end_date) {
    whereConditions.push("p.payment_date BETWEEN ? AND ?");
    queryParams.push(start_date, end_date);
  }

  if (class_id) {
    whereConditions.push("c.id = ?");
    queryParams.push(class_id);
  }

  if (payment_method) {
    whereConditions.push("p.payment_method = ?");
    queryParams.push(payment_method);
  }

  const [payments] = await pool.query(
    `
    SELECT 
      p.id as "Payment ID",
      r.receipt_number as "Receipt Number",
      CONCAT(s.first_name, ' ', s.last_name) as "Student Name",
      s.admission_number as "Admission Number",
      c.class_name as "Class",
      fc.category_name as "Fee Category",
      COALESCE(pa.description, bt.description, fc.category_name) as "Description",
      pa.amount_allocated as "Amount",
      p.payment_method as "Payment Method",
      p.reference_number as "Reference Number",
      DATE_FORMAT(p.payment_date, '%Y-%m-%d') as "Payment Date",
      DATE_FORMAT(r.issued_date, '%Y-%m-%d') as "Receipt Date",
      ay.year_label as "Academic Year",
      t.term_name as "Term",
      u.username as "Received By",
      p.notes as "Notes"
      
    FROM payments p
    
    INNER JOIN payment_allocations pa ON p.id = pa.payment_id
    INNER JOIN bills b ON pa.bill_id = b.id
    INNER JOIN bill_templates bt ON b.bill_template_id = bt.id
    INNER JOIN fee_categories fc ON bt.fee_category_id = fc.id
    INNER JOIN receipts r ON p.id = r.payment_id
    INNER JOIN students s ON p.student_id = s.id
    LEFT JOIN class_assignments ca ON s.id = ca.student_id AND r.academic_year_id = ca.academic_year_id
    LEFT JOIN classes c ON ca.class_id = c.id
    INNER JOIN academic_years ay ON r.academic_year_id = ay.id
    INNER JOIN terms t ON r.term_id = t.id
    INNER JOIN users u ON p.received_by = u.id
    
    WHERE ${whereConditions.join(" AND ")}
    ORDER BY p.payment_date DESC, p.id DESC
    `,
    queryParams,
  );

  return payments;
};

// Helper function to fetch student statements for export
const fetchStudentStatementsForExport = async (filters) => {
  const {
    student_id,
    admission_number,
    class_id,
    academic_year_id,
    term_id,
    status,
    student_name,
  } = filters;

  let whereConditions = ["1=1"];
  let queryParams = [];

  if (student_id) {
    whereConditions.push("s.id = ?");
    queryParams.push(student_id);
  }

  if (admission_number) {
    whereConditions.push("s.admission_number = ?");
    queryParams.push(admission_number);
  }

  if (student_name) {
    whereConditions.push(
      "(s.first_name LIKE ? OR s.last_name LIKE ? OR CONCAT(s.first_name, ' ', s.last_name) LIKE ?)",
    );
    queryParams.push(
      `%${student_name}%`,
      `%${student_name}%`,
      `%${student_name}%`,
    );
  }

  if (class_id) {
    whereConditions.push("c.id = ?");
    queryParams.push(class_id);
  }

  if (academic_year_id) {
    whereConditions.push("stb.academic_year_id = ?");
    queryParams.push(academic_year_id);
  }

  if (term_id) {
    whereConditions.push("stb.term_id = ?");
    queryParams.push(term_id);
  }

  if (status === "owing") {
    whereConditions.push(
      "(stb.remaining_balance > 0 OR stb.remaining_balance IS NULL)",
    );
  } else if (status === "fully_paid") {
    whereConditions.push("stb.remaining_balance <= 0");
  } else if (status === "pending") {
    whereConditions.push("(stb.total_amount > 0 AND stb.paid_amount = 0)");
  }

  // FIXED: Get current academic year ID
  const [currentYear] = await pool.query(
    "SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1",
  );
  const currentAcademicYearId = currentYear[0]?.id || null;

  // FIXED: Get latest term ID instead of current term (since terms table doesn't have is_current)
  const [latestTerm] = await pool.query(
    "SELECT id FROM terms ORDER BY start_date DESC LIMIT 1",
  );
  const latestTermId = latestTerm[0]?.id || null;

  const [students] = await pool.query(
    `
    SELECT 
      s.admission_number as "Admission Number",
      CONCAT(s.first_name, ' ', s.last_name) as "Student Name",
      s.parent_name as "Parent Name",
      s.parent_contact as "Parent Contact",
      c.class_name as "Class",
      ay.year_label as "Academic Year",
      t.term_name as "Term",
      stb.total_amount as "Total Bill",
      stb.paid_amount as "Amount Paid",
      stb.remaining_balance as "Balance",
      CASE 
        WHEN stb.remaining_balance > 0 THEN 'Owing'
        WHEN stb.remaining_balance <= 0 AND stb.remaining_balance IS NOT NULL THEN 'Fully Paid'
        WHEN stb.is_finalized IS NULL OR stb.is_finalized = FALSE THEN 'Pending'
        ELSE 'No Bill'
      END as "Status",
      DATE_FORMAT(stb.last_payment_date, '%Y-%m-%d') as "Last Payment Date",
      COALESCE(payment_summary.total_payments, 0) as "Payment Count",
      COALESCE(payment_summary.total_paid, 0) as "Total Paid to Date"
      
    FROM students s
    
    LEFT JOIN class_assignments ca ON s.id = ca.student_id 
      AND ca.academic_year_id = COALESCE(?, ?)
    LEFT JOIN classes c ON ca.class_id = c.id
    LEFT JOIN student_term_bills stb ON s.id = stb.student_id
      AND stb.is_finalized = TRUE
      AND (stb.academic_year_id = COALESCE(?, ?) OR ? IS NULL)
      AND (stb.term_id = COALESCE(?, ?) OR ? IS NULL)
    LEFT JOIN academic_years ay ON stb.academic_year_id = ay.id
    LEFT JOIN terms t ON stb.term_id = t.id
    LEFT JOIN (
      SELECT 
        student_id,
        COUNT(*) as total_payments,
        SUM(amount_paid) as total_paid
      FROM payments
      GROUP BY student_id
    ) payment_summary ON s.id = payment_summary.student_id
    
    WHERE ${whereConditions.join(" AND ")}
      AND (s.is_active IS NULL OR s.is_active = TRUE)
    
    ORDER BY c.class_name, s.first_name, s.last_name
    `,
    [
      academic_year_id,
      currentAcademicYearId, // For class_assignments
      academic_year_id,
      currentAcademicYearId,
      academic_year_id, // For stb academic_year_id
      term_id,
      latestTermId,
      term_id, // For stb term_id - USING latestTermId instead of is_current
      ...queryParams,
    ],
  );

  return students;
};

// Helper function to fetch class collections for export - FIXED VERSION
const fetchClassCollectionsForExport = async (filters) => {
  const { academic_year_id, term_id, start_date, end_date } = filters;

  let whereConditions = ["1=1"];
  let queryParams = [];

  if (academic_year_id) {
    whereConditions.push("r.academic_year_id = ?");
    queryParams.push(academic_year_id);
  }

  if (term_id) {
    whereConditions.push("r.term_id = ?");
    queryParams.push(term_id);
  }

  if (start_date && end_date) {
    whereConditions.push("p.payment_date BETWEEN ? AND ?");
    queryParams.push(start_date, end_date);
  }

  // Get current academic year ID if not provided
  const [currentYear] = await pool.query(
    "SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1",
  );
  const currentAcademicYearId = currentYear[0]?.id || null;

  // Get latest term ID if not provided - FIXED: No is_current column in terms
  const [latestTerm] = await pool.query(
    "SELECT id FROM terms ORDER BY start_date DESC LIMIT 1",
  );
  const latestTermId = latestTerm[0]?.id || null;

  const [classCollections] = await pool.query(
    `
    SELECT 
      c.class_name as "Class",
      COUNT(DISTINCT s.id) as "Total Students",
      COUNT(DISTINCT p.id) as "Total Payments",
      SUM(pa.amount_allocated) as "Total Collected",
      AVG(pa.amount_allocated) as "Average Payment",
      SUM(CASE WHEN stb.remaining_balance > 0 THEN 1 ELSE 0 END) as "Students Owing",
      SUM(CASE WHEN stb.remaining_balance <= 0 AND stb.remaining_balance IS NOT NULL THEN 1 ELSE 0 END) as "Students Fully Paid",
      SUM(CASE WHEN stb.is_finalized IS NULL OR stb.is_finalized = FALSE THEN 1 ELSE 0 END) as "Students Pending",
      ROUND(
        (SUM(CASE WHEN stb.remaining_balance <= 0 AND stb.remaining_balance IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(COUNT(DISTINCT s.id), 0)), 2
      ) as "Collection Rate (%)"
      
    FROM classes c
    
    INNER JOIN class_assignments ca ON c.id = ca.class_id
      AND ca.academic_year_id = COALESCE(?, ?)
    INNER JOIN students s ON ca.student_id = s.id
    LEFT JOIN payments p ON s.id = p.student_id
    LEFT JOIN payment_allocations pa ON p.id = pa.payment_id
    LEFT JOIN receipts r ON p.id = r.payment_id
      AND (r.academic_year_id = ca.academic_year_id OR r.academic_year_id IS NULL)
    LEFT JOIN student_term_bills stb ON s.id = stb.student_id
      AND stb.academic_year_id = ca.academic_year_id
      AND stb.term_id = COALESCE(?, ?) 
    
    WHERE ${whereConditions.join(" AND ")}
      AND (s.is_active IS NULL OR s.is_active = TRUE)
    
    GROUP BY c.id, c.class_name
    ORDER BY "Total Collected" DESC, c.class_name
    `,
    [
      academic_year_id,
      currentAcademicYearId,
      term_id,
      latestTermId,
      ...queryParams,
    ],
  );

  return classCollections;
};

// POST /api/cash-receipts - Record daily cash receipt
const recordCashReceipt = async (req, res) => {
  try {
    const {
      receipt_date,
      fee_category_id,
      amount,
      description,
      source,
      received_by,
      payment_method,
      reference_number,
      notes,
    } = req.body;

    // Validate required fields
    if (!receipt_date || !fee_category_id || !amount || !source) {
      return res.status(400).json({
        error:
          "Missing required fields: date, category, amount, and source are required",
      });
    }

    // Validate amount
    if (parseFloat(amount) <= 0) {
      return res.status(400).json({ error: "Amount must be greater than 0" });
    }

    const [result] = await pool.query(
      `INSERT INTO daily_cash_receipts 
       (receipt_date, fee_category_id, amount, description, source, 
        received_by, payment_method, reference_number, notes) 
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        receipt_date,
        fee_category_id,
        amount,
        description,
        source,
        received_by,
        payment_method || "Cash",
        reference_number,
        notes,
      ],
    );

    // Get the created record with category info
    const [newReceipt] = await pool.query(
      `SELECT cr.*, fc.category_name 
       FROM daily_cash_receipts cr
       JOIN fee_categories fc ON cr.fee_category_id = fc.id
       WHERE cr.id = ?`,
      [result.insertId],
    );

    res.status(201).json(newReceipt[0]);
  } catch (error) {
    console.error("Error recording cash receipt:", error);
    res.status(500).json({ error: "Failed to record cash receipt" });
  }
};

// GET /api/cash-receipts - Get cash receipts with filters
const getCashReceipts = async (req, res) => {
  try {
    const {
      start_date,
      end_date,
      fee_category_id,
      source,
      page = 1,
      limit = 50,
    } = req.query;
    const offset = (page - 1) * limit;

    let whereConditions = ["1=1"];
    let queryParams = [];

    if (start_date && end_date) {
      whereConditions.push("cr.receipt_date BETWEEN ? AND ?");
      queryParams.push(start_date, end_date);
    } else if (start_date) {
      whereConditions.push("cr.receipt_date >= ?");
      queryParams.push(start_date);
    } else if (end_date) {
      whereConditions.push("cr.receipt_date <= ?");
      queryParams.push(end_date);
    }

    if (fee_category_id) {
      whereConditions.push("cr.fee_category_id = ?");
      queryParams.push(fee_category_id);
    }

    if (source) {
      whereConditions.push("cr.source LIKE ?");
      queryParams.push(`%${source}%`);
    }

    // Get receipts with pagination
    const [receipts] = await pool.query(
      `SELECT cr.*, fc.category_name, u.username as received_by_name
       FROM daily_cash_receipts cr
       JOIN fee_categories fc ON cr.fee_category_id = fc.id
       JOIN users u ON cr.received_by = u.id
       WHERE ${whereConditions.join(" AND ")}
       ORDER BY cr.receipt_date DESC, cr.created_at DESC
       LIMIT ? OFFSET ?`,
      [...queryParams, parseInt(limit), offset],
    );

    // Get total count
    const [countResult] = await pool.query(
      `SELECT COUNT(*) as total 
       FROM daily_cash_receipts cr
       WHERE ${whereConditions.join(" AND ")}`,
      queryParams,
    );

    res.json({
      receipts,
      pagination: {
        page: parseInt(page),
        limit: parseInt(limit),
        total: countResult[0].total,
        pages: Math.ceil(countResult[0].total / limit),
      },
    });
  } catch (error) {
    console.error("Error fetching cash receipts:", error);
    res.status(500).json({ error: "Failed to fetch cash receipts" });
  }
};

// GET /api/cash-receipts/summary - Get daily summary
const getCashReceiptsSummary = async (req, res) => {
  try {
    const { start_date, end_date, group_by = "daily" } = req.query;

    let whereConditions = ["1=1"];
    let queryParams = [];
    let groupByClause = "receipt_date";

    if (start_date && end_date) {
      whereConditions.push("receipt_date BETWEEN ? AND ?");
      queryParams.push(start_date, end_date);
    }

    if (group_by === "category") {
      groupByClause = "fee_category_id, receipt_date";
    } else if (group_by === "source") {
      groupByClause = "source, receipt_date";
    }

    const [summary] = await pool.query(
      `SELECT 
         receipt_date,
         fee_category_id,
         fc.category_name,
         source,
         COUNT(*) as total_transactions,
         SUM(amount) as total_amount,
         GROUP_CONCAT(DISTINCT source) as sources_list
       FROM daily_cash_receipts cr
       LEFT JOIN fee_categories fc ON cr.fee_category_id = fc.id
       WHERE ${whereConditions.join(" AND ")}
       GROUP BY ${groupByClause}
       ORDER BY receipt_date DESC, total_amount DESC`,
      queryParams,
    );

    res.json(summary);
  } catch (error) {
    console.error("Error fetching cash summary:", error);
    res.status(500).json({ error: "Failed to fetch cash summary" });
  }
};

// GET /api/cash-receipts/stats - Get statistics
const getCashReceiptsStats = async (req, res) => {
  try {
    const { start_date, end_date } = req.query;

    let whereConditions = ["1=1"];
    let queryParams = [];

    if (start_date && end_date) {
      whereConditions.push("receipt_date BETWEEN ? AND ?");
      queryParams.push(start_date, end_date);
    }

    const [stats] = await pool.query(
      `SELECT 
         COUNT(*) as total_receipts,
         SUM(amount) as total_amount,
         AVG(amount) as average_amount,
         MIN(amount) as min_amount,
         MAX(amount) as max_amount,
         COUNT(DISTINCT source) as unique_sources,
         COUNT(DISTINCT fee_category_id) as unique_categories,
         MIN(receipt_date) as start_date,
         MAX(receipt_date) as end_date
       FROM daily_cash_receipts
       WHERE ${whereConditions.join(" AND ")}`,
      queryParams,
    );

    // Get top categories
    const [topCategories] = await pool.query(
      `SELECT 
         fc.category_name,
         COUNT(*) as receipt_count,
         SUM(amount) as total_amount
       FROM daily_cash_receipts cr
       JOIN fee_categories fc ON cr.fee_category_id = fc.id
       WHERE ${whereConditions.join(" AND ")}
       GROUP BY fee_category_id, fc.category_name
       ORDER BY total_amount DESC
       LIMIT 5`,
      queryParams,
    );

    // Get top sources
    const [topSources] = await pool.query(
      `SELECT 
         source,
         COUNT(*) as receipt_count,
         SUM(amount) as total_amount
       FROM daily_cash_receipts
       WHERE ${whereConditions.join(" AND ")}
       GROUP BY source
       ORDER BY total_amount DESC
       LIMIT 5`,
      queryParams,
    );

    res.json({
      overview: stats[0] || {},
      top_categories: topCategories,
      top_sources: topSources,
    });
  } catch (error) {
    console.error("Error fetching cash stats:", error);
    res.status(500).json({ error: "Failed to fetch cash statistics" });
  }
};

// GET /api/cash-receipts/export - Export cash receipts
const exportCashReceipts = async (req, res) => {
  try {
    const {
      start_date,
      end_date,
      fee_category_id,
      source,
      format = "excel",
    } = req.query;

    console.log("Export request received:", {
      start_date,
      end_date,
      fee_category_id,
      source,
      format,
    });

    let whereConditions = ["1=1"];
    let queryParams = [];

    // Set default date to today if no dates provided
    const defaultStartDate =
      start_date || new Date().toISOString().split("T")[0];
    const defaultEndDate = end_date || new Date().toISOString().split("T")[0];

    whereConditions.push("cr.receipt_date BETWEEN ? AND ?");
    queryParams.push(defaultStartDate, defaultEndDate);

    if (
      fee_category_id &&
      fee_category_id !== "" &&
      fee_category_id !== "undefined"
    ) {
      whereConditions.push("cr.fee_category_id = ?");
      queryParams.push(fee_category_id);
    }

    if (source && source !== "" && source !== "undefined") {
      whereConditions.push("cr.source LIKE ?");
      queryParams.push(`%${source}%`);
    }

    console.log("Query conditions:", whereConditions);
    console.log("Query params:", queryParams);

    // Get data
    const [receipts] = await pool.query(
      `SELECT 
         cr.id as "Receipt ID",
         cr.receipt_date as "Date",
         fc.category_name as "Fee Category",
         cr.source as "Source",
         cr.description as "Description",
         cr.amount as "Amount",
         cr.payment_method as "Payment Method",
         cr.reference_number as "Reference Number",
         u.username as "Recorded By",
         DATE_FORMAT(cr.created_at, '%Y-%m-%d %H:%i:%s') as "Recorded At",
         cr.notes as "Notes"
       FROM daily_cash_receipts cr
       JOIN fee_categories fc ON cr.fee_category_id = fc.id
       JOIN users u ON cr.received_by = u.id
       WHERE ${whereConditions.join(" AND ")}
       ORDER BY cr.receipt_date DESC, cr.id DESC`,
      queryParams,
    );

    console.log(`Found ${receipts.length} receipts for export`);

    // Get summary
    const [summary] = await pool.query(
      `SELECT 
         COUNT(*) as "Total Receipts",
         SUM(amount) as "Total Amount",
         AVG(amount) as "Average Amount",
         MIN(receipt_date) as "Start Date",
         MAX(receipt_date) as "End Date"
       FROM daily_cash_receipts cr
       WHERE ${whereConditions.join(" AND ")}`,
      queryParams,
    );

    // Export based on format
    if (format === "pdf") {
      await exportCashReceiptsPDF(res, receipts, summary[0] || {});
    } else {
      await exportCashReceiptsExcel(res, receipts, summary[0] || {});
    }
  } catch (error) {
    console.error("Error exporting cash receipts:", error);
    res
      .status(500)
      .json({ error: "Failed to export cash receipts: " + error.message });
  }
};

// Helper: Export to Excel
const exportCashReceiptsExcel = async (res, receipts, summary) => {
  try {
    const workbook = XLSX.utils.book_new();

    // Helper function to format dates
    const formatDateForExcel = (dateStr) => {
      if (!dateStr || dateStr === "N/A") return "";
      try {
        const date = new Date(dateStr);
        return date.toLocaleDateString("en-US", {
          year: "numeric",
          month: "2-digit",
          day: "2-digit",
        });
      } catch (error) {
        return dateStr;
      }
    };

    // Format dates for Excel BEFORE creating the worksheet
    const formattedReceipts = receipts.map((receipt) => ({
      ...receipt,
      Date: formatDateForExcel(receipt["Date"]),
      // Also format Recorded At if it exists
      "Recorded At": receipt["Recorded At"]
        ? formatDateForExcel(receipt["Recorded At"].split(" ")[0]) +
          (receipt["Recorded At"].includes(" ")
            ? " " + receipt["Recorded At"].split(" ")[1]
            : "")
        : "",
    }));

    // Main data sheet
    const worksheet = XLSX.utils.json_to_sheet(formattedReceipts);

    // Set column widths for better readability
    const colWidths = [
      { wch: 12 }, // Date
      { wch: 20 }, // Category
      { wch: 20 }, // Source
      { wch: 30 }, // Description
      { wch: 15 }, // Amount
      { wch: 15 }, // Method
      { wch: 15 }, // Reference
      { wch: 20 }, // Recorded By
      { wch: 20 }, // Recorded At
    ];
    worksheet["!cols"] = colWidths;

    // Add header styling (optional but nice)
    const headerRange = XLSX.utils.decode_range(worksheet["!ref"]);
    for (let C = headerRange.s.c; C <= headerRange.e.c; ++C) {
      const address = XLSX.utils.encode_cell({ r: 0, c: C });
      if (!worksheet[address]) continue;
      // Make headers bold
      worksheet[address].s = { font: { bold: true } };
    }

    XLSX.utils.book_append_sheet(workbook, worksheet, "Cash Receipts");

    // Generate the Excel buffer
    const buffer = XLSX.write(workbook, {
      type: "buffer",
      bookType: "xlsx",
    });

    // Send the file with proper .xlsx extension
    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    );
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="cash-receipts-${
        new Date().toISOString().split("T")[0]
      }.xlsx"`,
    );
    res.send(buffer);
  } catch (error) {
    console.error("Error creating Excel file:", error);
    throw error;
  }
};

// Helper: Export to PDF - UPDATED with proper date formatting
const exportCashReceiptsPDF = async (res, receipts, summary) => {
  try {
    const doc = new jsPDF();
    const pageWidth = doc.internal.pageSize.getWidth();
    const pageHeight = doc.internal.pageSize.getHeight();
    const schoolSettings = await getSchoolSettingsForPDF();

    // Colors
    const primaryColor = [41, 128, 185];
    const secondaryColor = [52, 152, 219];

    // Header
    doc.setFillColor(...primaryColor);
    doc.rect(0, 0, pageWidth, 30, "F");
    const hasLogo = await addSchoolLogoToPDF(doc, 15, 3, 25, 25);

    // School name
    doc.setTextColor(255, 255, 255);
    doc.setFontSize(20);
    doc.setFont("helvetica", "bold");
    doc.text(schoolSettings.school_name, hasLogo ? 50 : pageWidth / 2, 12, {
      align: hasLogo ? "left" : "center",
    });

    // Report title
    doc.setFontSize(16);
    doc.text("DAILY CASH RECEIPTS REPORT", pageWidth / 2, 22, {
      align: "center",
    });

    // Report period
    doc.setFontSize(10);
    doc.setFont("helvetica", "normal");

    // FIX: Properly format dates
    const formatDate = (dateStr) => {
      if (!dateStr) return "N/A";
      try {
        const date = new Date(dateStr);
        return date.toLocaleDateString("en-US", {
          year: "numeric",
          month: "short",
          day: "numeric",
        });
      } catch (error) {
        return dateStr;
      }
    };

    doc.text(
      `Period: ${formatDate(summary["Start Date"])} to ${formatDate(
        summary["End Date"],
      )}`,
      20,
      40,
    );
    doc.text(
      `Generated: ${new Date().toLocaleDateString()}`,
      pageWidth - 20,
      40,
      {
        align: "right",
      },
    );

    // Summary section
    let yPos = 50;
    doc.setFontSize(12);
    doc.setFont("helvetica", "bold");
    doc.text("SUMMARY", 20, yPos);
    yPos += 8;

    doc.setFontSize(10);
    doc.setFont("helvetica", "normal");
    const summaryLines = [
      `Total Receipts: ${summary["Total Receipts"] || 0}`,
      `Total Amount: Ghc ${parseFloat(summary["Total Amount"] || 0).toFixed(
        2,
      )}`,
      `Average Amount: Ghc ${parseFloat(summary["Average Amount"] || 0).toFixed(
        2,
      )}`,
    ];

    summaryLines.forEach((line, index) => {
      doc.text(line, 20, yPos + index * 5);
    });

    yPos += summaryLines.length * 5 + 10;

    // FIX: Format table dates properly
    const tableData = receipts.map((receipt) => [
      formatDate(receipt["Date"]), // FIXED: Now properly formatted
      receipt["Fee Category"],
      receipt["Source"],
      receipt["Description"] || "",
      `Ghc ${parseFloat(receipt["Amount"] || 0).toFixed(2)}`,
      receipt["Payment Method"],
      receipt["Recorded By"],
    ]);

    // Generate table
    autoTable(doc, {
      startY: yPos,
      head: [
        [
          "Date",
          "Category",
          "Source",
          "Description",
          "Amount",
          "Method",
          "Recorded By",
        ],
      ],
      body: tableData,
      headStyles: {
        fillColor: primaryColor,
        textColor: [255, 255, 255],
        fontStyle: "bold",
        fontSize: 9,
      },
      bodyStyles: {
        fontSize: 8,
        cellPadding: 2,
      },
      alternateRowStyles: {
        fillColor: [248, 248, 248],
      },
      styles: {
        overflow: "linebreak",
        cellWidth: "wrap",
      },
      columnStyles: {
        0: { cellWidth: 25 }, // Date column width
        1: { cellWidth: 30 }, // Category column width
        2: { cellWidth: 25 }, // Source column width
        3: { cellWidth: 40 }, // Description column width (auto)
        4: { cellWidth: 20 }, // Amount column width
        5: { cellWidth: 20 }, // Method column width
        6: { cellWidth: 25 }, // Recorded By column width
      },
      margin: { left: 10, right: 10 },
      didDrawPage: (data) => {
        // Page numbers
        const pageCount = doc.internal.getNumberOfPages();
        doc.setFontSize(8);
        doc.setTextColor(100, 100, 100);
        doc.text(
          `Page ${data.pageNumber} of ${pageCount}`,
          pageWidth / 2,
          pageHeight - 10,
          { align: "center" },
        );
      },
    });

    // Footer
    const finalY = doc.lastAutoTable.finalY + 10;
    if (finalY < pageHeight - 20) {
      doc.setFontSize(8);
      doc.setTextColor(100, 100, 100);
      doc.text(
        schoolSettings.school_short_name + " - Daily Cash Receipts Report",
        pageWidth / 2,
        finalY,
        { align: "center" },
      );
    }

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="cash-receipts-${
        new Date().toISOString().split("T")[0]
      }.pdf"`,
    );
    res.send(Buffer.from(doc.output("arraybuffer")));
  } catch (error) {
    console.error("Error creating PDF:", error);
    throw error;
  }
};

// DELETE /api/cash-receipts/:id - Delete cash receipt
const deleteCashReceipt = async (req, res) => {
  try {
    const { id } = req.params;

    const [result] = await pool.query(
      "DELETE FROM daily_cash_receipts WHERE id = ?",
      [id],
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ error: "Cash receipt not found" });
    }

    res.json({ message: "Cash receipt deleted successfully" });
  } catch (error) {
    console.error("Error deleting cash receipt:", error);
    res.status(500).json({ error: "Failed to delete cash receipt" });
  }
};

//  EXPENSES MANAGEMENT CONTROLLERS ==

// GET /api/expenses - Get all expenses with filters
const getExpenses = async (req, res) => {
  try {
    const {
      start_date,
      end_date,
      expense_category,
      paid_to,
      min_amount,
      max_amount,
      recorded_by,
      page = 1,
      limit = 50,
    } = req.query;

    let whereConditions = ["1=1"];
    let queryParams = [];
    const offset = (page - 1) * limit;

    // Date range filter
    if (start_date && end_date) {
      whereConditions.push("expense_date BETWEEN ? AND ?");
      queryParams.push(start_date, end_date);
    } else if (start_date) {
      whereConditions.push("expense_date >= ?");
      queryParams.push(start_date);
    } else if (end_date) {
      whereConditions.push("expense_date <= ?");
      queryParams.push(end_date);
    }

    if (expense_category) {
      whereConditions.push("expense_category = ?");
      queryParams.push(expense_category);
    }

    if (paid_to) {
      whereConditions.push("paid_to LIKE ?");
      queryParams.push(`%${paid_to}%`);
    }

    if (min_amount) {
      whereConditions.push("amount >= ?");
      queryParams.push(min_amount);
    }

    if (max_amount) {
      whereConditions.push("amount <= ?");
      queryParams.push(max_amount);
    }

    if (recorded_by) {
      whereConditions.push("recorded_by = ?");
      queryParams.push(recorded_by);
    }

    // Get expenses with pagination
    const [expenses] = await pool.query(
      `SELECT e.*, u.username as recorded_by_name
       FROM expenses e
       LEFT JOIN users u ON e.recorded_by = u.id
       WHERE ${whereConditions.join(" AND ")}
       ORDER BY e.expense_date DESC, e.id DESC
       LIMIT ? OFFSET ?`,
      [...queryParams, parseInt(limit), offset],
    );

    // Get total count
    const [countResult] = await pool.query(
      `SELECT COUNT(*) as total FROM expenses e
       WHERE ${whereConditions.join(" AND ")}`,
      queryParams,
    );

    // Get expense categories for filter options
    const [categories] = await pool.query(
      `SELECT DISTINCT expense_category FROM expenses 
       WHERE expense_category IS NOT NULL AND expense_category != ''
       ORDER BY expense_category`,
    );

    res.json({
      expenses,
      categories: categories.map((c) => c.expense_category),
      pagination: {
        page: parseInt(page),
        limit: parseInt(limit),
        total: countResult[0].total,
        pages: Math.ceil(countResult[0].total / limit),
      },
    });
  } catch (error) {
    console.error("Error fetching expenses:", error);
    res.status(500).json({ error: "Failed to fetch expenses" });
  }
};

// GET /api/expenses/:id - Get specific expense
const getExpenseById = async (req, res) => {
  try {
    const { id } = req.params;

    const [expenses] = await pool.query(
      `SELECT e.*, u.username as recorded_by_name
       FROM expenses e
       LEFT JOIN users u ON e.recorded_by = u.id
       WHERE e.id = ?`,
      [id],
    );

    if (expenses.length === 0) {
      return res.status(404).json({ error: "Expense not found" });
    }

    res.json(expenses[0]);
  } catch (error) {
    console.error("Error fetching expense:", error);
    res.status(500).json({ error: "Failed to fetch expense" });
  }
};

// POST /api/expenses - Create new expense/PCV
const createExpense = async (req, res) => {
  try {
    const {
      expense_category,
      amount,
      expense_date,
      description,
      paid_to,
      recorded_by,
      payment_method,
      reference_number,
    } = req.body;

    // Validate required fields
    if (
      !expense_category ||
      !amount ||
      !expense_date ||
      !description ||
      !paid_to ||
      !recorded_by
    ) {
      return res.status(400).json({
        error:
          "Missing required fields: category, amount, date, description, paid_to, and recorded_by are required",
      });
    }

    // First, check if expenses table has voucher_number column
    let hasVoucherNumber = false;
    try {
      const [columns] = await pool.query(`
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'expenses' 
        AND COLUMN_NAME = 'voucher_number'
        AND TABLE_SCHEMA = DATABASE()
      `);
      hasVoucherNumber = columns.length > 0;
    } catch (error) {
      console.error(
        "Could not check for voucher_number column:",
        error.message,
      );
    }

    // If voucher_number column exists, try to generate one
    let voucher_number = "";
    if (hasVoucherNumber) {
      try {
        const today = new Date();
        const year = today.getFullYear();
        const month = String(today.getMonth() + 1).padStart(2, "0");
        const [lastVoucher] = await pool.query(
          "SELECT voucher_number FROM expenses WHERE voucher_number LIKE ? ORDER BY id DESC LIMIT 1",
          [`PV-${year}${month}-%`],
        );

        let sequence = 1;
        if (lastVoucher.length > 0) {
          const lastSeq =
            parseInt(lastVoucher[0].voucher_number.split("-")[2]) || 0;
          sequence = lastSeq + 1;
        }

        voucher_number = `PV-${year}${month}-${String(sequence).padStart(
          4,
          "0",
        )}`;
      } catch (voucherError) {
        console.error(
          "Could not generate voucher number:",
          voucherError.message,
        );
        voucher_number = "";
      }
    }

    // Build the query based on available columns
    const queryColumns = [
      "expense_category",
      "amount",
      "expense_date",
      "description",
      "paid_to",
      "recorded_by",
    ];
    const queryValues = [
      expense_category,
      amount,
      expense_date,
      description,
      paid_to,
      recorded_by,
    ];

    // Add optional columns if they exist
    const optionalColumns = [
      "payment_method",
      "reference_number",
      "voucher_number",
    ];
    const optionalValues = [payment_method, reference_number, voucher_number];

    for (let i = 0; i < optionalColumns.length; i++) {
      const columnName = optionalColumns[i];
      const columnValue = optionalValues[i];

      if (
        columnValue !== undefined &&
        columnValue !== null &&
        columnValue !== ""
      ) {
        queryColumns.push(columnName);
        queryValues.push(columnValue);
      }
    }

    // Create the INSERT query
    const placeholders = queryColumns.map(() => "?").join(", ");
    const query = `INSERT INTO expenses (${queryColumns.join(
      ", ",
    )}) VALUES (${placeholders})`;

    const [result] = await pool.query(query, queryValues);

    // Get the created expense with recorded_by name
    const [newExpense] = await pool.query(
      `SELECT e.*, u.username as recorded_by_name
       FROM expenses e
       LEFT JOIN users u ON e.recorded_by = u.id
       WHERE e.id = ?`,
      [result.insertId],
    );

    res.status(201).json(newExpense[0]);
  } catch (error) {
    console.error("Error creating expense:", error);

    // More specific error messages
    if (error.code === "ER_BAD_FIELD_ERROR") {
      return res.status(400).json({
        error:
          "Database schema mismatch. Please update expenses table with required columns.",
        details: error.message,
      });
    }

    res.status(500).json({ error: "Failed to create expense" });
  }
};

// PUT /api/expenses/:id - Update expense
const updateExpense = async (req, res) => {
  try {
    const { id } = req.params;
    const {
      expense_category,
      amount,
      expense_date,
      description,
      paid_to,
      voucher_number,
      payment_method,
      reference_number,
    } = req.body;

    // Check if expense exists
    const [existing] = await pool.query(
      "SELECT id FROM expenses WHERE id = ?",
      [id],
    );

    if (existing.length === 0) {
      return res.status(404).json({ error: "Expense not found" });
    }

    // Build the update query dynamically
    const updates = [];
    const values = [];

    const fields = [
      { name: "expense_category", value: expense_category },
      { name: "amount", value: amount },
      { name: "expense_date", value: expense_date },
      { name: "description", value: description },
      { name: "paid_to", value: paid_to },
      { name: "voucher_number", value: voucher_number },
      { name: "payment_method", value: payment_method },
      { name: "reference_number", value: reference_number },
    ];

    fields.forEach((field) => {
      if (field.value !== undefined) {
        updates.push(`${field.name} = ?`);
        values.push(field.value);
      }
    });

    if (updates.length === 0) {
      return res.status(400).json({ error: "No fields to update" });
    }

    values.push(id);

    const query = `UPDATE expenses SET ${updates.join(", ")} WHERE id = ?`;
    await pool.query(query, values);

    const [updatedExpense] = await pool.query(
      `SELECT e.*, u.username as recorded_by_name
       FROM expenses e
       LEFT JOIN users u ON e.recorded_by = u.id
       WHERE e.id = ?`,
      [id],
    );

    res.json(updatedExpense[0]);
  } catch (error) {
    console.error("Error updating expense:", error);
    res.status(500).json({ error: "Failed to update expense" });
  }
};

// DELETE /api/expenses/:id - Delete expense
const deleteExpense = async (req, res) => {
  try {
    const { id } = req.params;

    const [result] = await pool.query("DELETE FROM expenses WHERE id = ?", [
      id,
    ]);

    if (result.affectedRows === 0) {
      return res.status(404).json({ error: "Expense not found" });
    }

    res.json({ message: "Expense deleted successfully" });
  } catch (error) {
    console.error("Error deleting expense:", error);
    res.status(500).json({ error: "Failed to delete expense" });
  }
};

// GET /api/expenses/statistics - Get expense statistics
const getExpenseStatistics = async (req, res) => {
  try {
    const { start_date, end_date } = req.query;

    let whereConditions = ["1=1"];
    let queryParams = [];

    if (start_date && end_date) {
      whereConditions.push("expense_date BETWEEN ? AND ?");
      queryParams.push(start_date, end_date);
    }

    const [stats] = await pool.query(
      `SELECT 
         COUNT(*) as total_expenses,
         SUM(amount) as total_amount,
         AVG(amount) as average_amount,
         MIN(amount) as min_amount,
         MAX(amount) as max_amount,
         COUNT(DISTINCT expense_category) as unique_categories,
         COUNT(DISTINCT paid_to) as unique_vendors,
         MIN(expense_date) as start_date,
         MAX(expense_date) as end_date
       FROM expenses
       WHERE ${whereConditions.join(" AND ")}`,
      queryParams,
    );

    // Get category breakdown
    const [categoryBreakdown] = await pool.query(
      `SELECT 
         expense_category,
         COUNT(*) as expense_count,
         SUM(amount) as total_amount
       FROM expenses
       WHERE ${whereConditions.join(" AND ")}
       GROUP BY expense_category
       ORDER BY total_amount DESC
       LIMIT 10`,
      queryParams,
    );

    // Get monthly trend
    const [monthlyTrend] = await pool.query(
      `SELECT 
         DATE_FORMAT(expense_date, '%Y-%m') as month_year,
         DATE_FORMAT(expense_date, '%M %Y') as month_name,
         COUNT(*) as expense_count,
         SUM(amount) as total_amount
       FROM expenses
       WHERE ${whereConditions.join(" AND ")}
       GROUP BY DATE_FORMAT(expense_date, '%Y-%m'), DATE_FORMAT(expense_date, '%M %Y')
       ORDER BY month_year DESC
       LIMIT 6`,
      queryParams,
    );

    // Get top vendors
    const [topVendors] = await pool.query(
      `SELECT 
         paid_to,
         COUNT(*) as expense_count,
         SUM(amount) as total_amount
       FROM expenses
       WHERE ${whereConditions.join(" AND ")}
       GROUP BY paid_to
       ORDER BY total_amount DESC
       LIMIT 10`,
      queryParams,
    );

    res.json({
      overview: stats[0] || {},
      category_breakdown: categoryBreakdown,
      monthly_trend: monthlyTrend,
      top_vendors: topVendors,
    });
  } catch (error) {
    console.error("Error fetching expense statistics:", error);
    res.status(500).json({ error: "Failed to fetch expense statistics" });
  }
};

// GET /api/expenses/categories - Get expense categories summary
const getExpenseCategories = async (req, res) => {
  try {
    const { start_date, end_date } = req.query;

    let whereConditions = ["1=1"];
    let queryParams = [];

    if (start_date && end_date) {
      whereConditions.push("expense_date BETWEEN ? AND ?");
      queryParams.push(start_date, end_date);
    }

    const [categories] = await pool.query(
      `SELECT 
         expense_category,
         COUNT(*) as transaction_count,
         SUM(amount) as total_amount,
         AVG(amount) as average_amount,
         MIN(expense_date) as first_transaction,
         MAX(expense_date) as last_transaction
       FROM expenses
       WHERE ${whereConditions.join(" AND ")}
         AND expense_category IS NOT NULL 
         AND expense_category != ''
       GROUP BY expense_category
       ORDER BY total_amount DESC`,
      queryParams,
    );

    res.json(categories);
  } catch (error) {
    console.error("Error fetching expense categories:", error);
    res.status(500).json({ error: "Failed to fetch expense categories" });
  }
};

// GET /api/expenses/export - Export expenses to Excel/PDF
const exportExpenses = async (req, res) => {
  try {
    const {
      start_date,
      end_date,
      expense_category,
      format = "excel",
    } = req.query;

    let whereConditions = ["1=1"];
    let queryParams = [];

    // Set default date to today if no dates provided
    const defaultStartDate =
      start_date || new Date().toISOString().split("T")[0];
    const defaultEndDate = end_date || new Date().toISOString().split("T")[0];

    whereConditions.push("expense_date BETWEEN ? AND ?");
    queryParams.push(defaultStartDate, defaultEndDate);

    if (
      expense_category &&
      expense_category !== "" &&
      expense_category !== "undefined"
    ) {
      whereConditions.push("expense_category = ?");
      queryParams.push(expense_category);
    }

    // Get expenses data
    const [expenses] = await pool.query(
      `SELECT 
         e.id as "Expense ID",
         e.voucher_number as "Voucher Number",
         e.expense_date as "Date",
         e.expense_category as "Category",
         e.description as "Description",
         e.paid_to as "Paid To",
         e.amount as "Amount",
         e.payment_method as "Payment Method",
         e.reference_number as "Reference Number",
         u.username as "Recorded By",
         DATE_FORMAT(e.created_at, '%Y-%m-%d %H:%i:%s') as "Recorded At"
       FROM expenses e
       LEFT JOIN users u ON e.recorded_by = u.id
       WHERE ${whereConditions.join(" AND ")}
       ORDER BY e.expense_date DESC, e.id DESC`,
      queryParams,
    );

    // Get summary statistics
    const [summary] = await pool.query(
      `SELECT 
         COUNT(*) as "Total Expenses",
         SUM(amount) as "Total Amount",
         AVG(amount) as "Average Amount",
         MIN(expense_date) as "Start Date",
         MAX(expense_date) as "End Date"
       FROM expenses e
       WHERE ${whereConditions.join(" AND ")}`,
      queryParams,
    );

    // Export based on format
    if (format === "pdf") {
      await exportExpensesPDF(res, expenses, summary[0] || {});
    } else {
      await exportExpensesExcel(res, expenses, summary[0] || {});
    }
  } catch (error) {
    console.error("Error exporting expenses:", error);
    res
      .status(500)
      .json({ error: "Failed to export expenses: " + error.message });
  }
};

// Helper: Export expenses to Excel
const exportExpensesExcel = async (res, expenses, summary) => {
  try {
    const workbook = XLSX.utils.book_new();

    // Format dates for Excel
    const formatDateForExcel = (dateStr) => {
      if (!dateStr || dateStr === "N/A") return "";
      try {
        const date = new Date(dateStr);
        return date.toLocaleDateString("en-US", {
          year: "numeric",
          month: "2-digit",
          day: "2-digit",
        });
      } catch (error) {
        return dateStr;
      }
    };

    const formattedExpenses = expenses.map((expense) => ({
      ...expense,
      Date: formatDateForExcel(expense["Date"]),
      "Recorded At": expense["Recorded At"]
        ? formatDateForExcel(expense["Recorded At"].split(" ")[0]) +
          (expense["Recorded At"].includes(" ")
            ? " " + expense["Recorded At"].split(" ")[1]
            : "")
        : "",
    }));

    // Main data sheet
    const worksheet = XLSX.utils.json_to_sheet(formattedExpenses);

    // Set column widths
    const colWidths = [
      { wch: 12 }, // Expense ID
      { wch: 15 }, // Voucher Number
      { wch: 12 }, // Date
      { wch: 20 }, // Category
      { wch: 30 }, // Description
      { wch: 20 }, // Paid To
      { wch: 15 }, // Amount
      { wch: 15 }, // Payment Method
      { wch: 15 }, // Reference Number
      { wch: 20 }, // Recorded By
      { wch: 20 }, // Recorded At
    ];
    worksheet["!cols"] = colWidths;

    // Make headers bold
    const headerRange = XLSX.utils.decode_range(worksheet["!ref"]);
    for (let C = headerRange.s.c; C <= headerRange.e.c; ++C) {
      const address = XLSX.utils.encode_cell({ r: 0, c: C });
      if (!worksheet[address]) continue;
      worksheet[address].s = { font: { bold: true } };
    }

    XLSX.utils.book_append_sheet(workbook, worksheet, "Expenses");

    const buffer = XLSX.write(workbook, {
      type: "buffer",
      bookType: "xlsx",
    });

    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    );
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="expenses-${
        new Date().toISOString().split("T")[0]
      }.xlsx"`,
    );
    res.send(buffer);
  } catch (error) {
    console.error("Error creating Excel file:", error);
    throw error;
  }
};

// Helper: Export expenses to PDF
const exportExpensesPDF = async (res, expenses, summary) => {
  try {
    const doc = new jsPDF();
    const pageWidth = doc.internal.pageSize.getWidth();
    const pageHeight = doc.internal.pageSize.getHeight();
    const schoolSettings = await getSchoolSettingsForPDF();
    const primaryColor = [41, 128, 185];

    // Header with logo on left
    const headerHeight = 30;
    doc.setFillColor(41, 128, 185);
    doc.rect(0, 0, pageWidth, headerHeight, "F");

    // Logo on left (if exists)
    const hasLogo = await addSchoolLogoToPDF(doc, 15, 5, 20, 20);

    doc.setTextColor(255, 255, 255);
    doc.setFontSize(20);
    doc.setFont("helvetica", "bold");
    doc.text(schoolSettings.school_name, hasLogo ? 50 : pageWidth / 2, 12, {
      align: hasLogo ? "left" : "center",
    });

    doc.setFontSize(16);
    doc.text("EXPENSES / PV REPORT", pageWidth / 2, 22, { align: "center" });

    // Format date helper
    const formatDate = (dateStr) => {
      if (!dateStr) return "N/A";
      try {
        const date = new Date(dateStr);
        return date.toLocaleDateString("en-US", {
          year: "numeric",
          month: "short",
          day: "numeric",
        });
      } catch (error) {
        return dateStr;
      }
    };

    // Report period
    doc.setFontSize(10);
    doc.setTextColor(0, 0, 0);
    doc.setFont("helvetica", "normal");

    doc.text(
      `Period: ${formatDate(summary["Start Date"])} to ${formatDate(
        summary["End Date"],
      )}`,
      20,
      40,
    );
    doc.text(
      `Generated: ${new Date().toLocaleDateString()}`,
      pageWidth - 20,
      40,
      {
        align: "right",
      },
    );

    // Summary section
    let yPos = 50;
    doc.setFontSize(12);
    doc.setFont("helvetica", "bold");
    doc.text("SUMMARY", 20, yPos);
    yPos += 8;

    doc.setFontSize(10);
    doc.setFont("helvetica", "normal");
    const summaryLines = [
      `Total Expenses: ${summary["Total Expenses"] || 0}`,
      `Total Amount: Ghc ${parseFloat(summary["Total Amount"] || 0).toFixed(
        2,
      )}`,
      `Average Amount: Ghc ${parseFloat(summary["Average Amount"] || 0).toFixed(
        2,
      )}`,
    ];

    summaryLines.forEach((line, index) => {
      doc.text(line, 20, yPos + index * 5);
    });

    yPos += summaryLines.length * 5 + 10;

    // Table data
    const tableData = expenses.map((expense) => [
      expense["Voucher Number"] || "",
      formatDate(expense["Date"]),
      expense["Category"] || "",
      expense["Description"]?.substring(0, 30) +
        (expense["Description"]?.length > 30 ? "..." : "") || "",
      expense["Paid To"]?.substring(0, 20) +
        (expense["Paid To"]?.length > 20 ? "..." : "") || "",
      `Ghc ${parseFloat(expense["Amount"] || 0).toFixed(2)}`,
      expense["Payment Method"] || "",
    ]);

    // Generate table
    autoTable(doc, {
      startY: yPos,
      head: [
        [
          "Voucher No",
          "Date",
          "Category",
          "Description",
          "Paid To",
          "Amount",
          "Method",
        ],
      ],
      body: tableData,
      headStyles: {
        fillColor: primaryColor,
        textColor: [255, 255, 255],
        fontStyle: "bold",
        fontSize: 9,
      },
      bodyStyles: {
        fontSize: 8,
        cellPadding: 2,
      },
      alternateRowStyles: {
        fillColor: [248, 248, 248],
      },
      styles: {
        overflow: "linebreak",
        cellWidth: "wrap",
      },
      margin: { left: 10, right: 10 },
      didDrawPage: (data) => {
        const pageCount = doc.internal.getNumberOfPages();
        doc.setFontSize(8);
        doc.setTextColor(100, 100, 100);
        doc.text(
          `Page ${data.pageNumber} of ${pageCount}`,
          pageWidth / 2,
          pageHeight - 10,
          { align: "center" },
        );
      },
    });

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="expenses-${
        new Date().toISOString().split("T")[0]
      }.pdf"`,
    );
    res.send(Buffer.from(doc.output("arraybuffer")));
  } catch (error) {
    console.error("Error creating PDF:", error);
    throw error;
  }
};

// School Settings Controllers
// GET /api/school-settings - Get school settings
const getSchoolSettings = async (req, res) => {
  try {
    const [settings] = await pool.query(`
      SELECT ss.*, u.username as updated_by_name
      FROM school_settings ss
      LEFT JOIN users u ON ss.updated_by = u.id
      ORDER BY ss.id DESC
      LIMIT 1
    `);

    if (settings.length === 0) {
      // Return default settings if none exist
      return res.json({
        id: null,
        school_name: "School Manager Academy",
        school_short_name: "SMA",
        motto: "Quality Education for All",
        address: "123 Education Street, Learning City",
        city: "",
        region: "",
        postal_code: "",
        phone_numbers: '["(233) 123-4567"]',
        email: "info@sma.edu.gh",
        website: "",
        principal_name: "",
        registration_number: "",
        bank_name: "",
        branch_name: "",
        account_number: "",
        account_name: "",
        swift_code: "",
        mobile_money_provider: "",
        mobile_money_number: "",
        currency_symbol: "Ghc",
        receipt_footer: "Thank you for your payment!",
        bill_terms:
          "All payments should be made by the due date to avoid penalties.",
        late_fee_percentage: 5.0,
        updated_at: new Date().toISOString(),
        updated_by_name: "System",
      });
    }

    res.json(settings[0]);
  } catch (error) {
    console.error("Error fetching school settings:", error);
    res.status(500).json({ error: "Failed to fetch school settings" });
  }
};

// POST /api/school-settings - Create or update school settings
const updateSchoolSettings = async (req, res) => {
  const connection = await pool.getConnection();

  try {
    await connection.beginTransaction();

    // Handle file upload if present
    let logo_filename = null;
    if (req.file) {
      logo_filename = req.file.filename;

      // Delete old logo if exists
      const [existing] = await connection.query(
        "SELECT logo_filename FROM school_settings ORDER BY id DESC LIMIT 1",
      );

      if (existing.length > 0 && existing[0].logo_filename) {
        const oldLogoPath = path.join(
          __dirname,
          "../uploads/school-logo",
          existing[0].logo_filename,
        );
        if (fs.existsSync(oldLogoPath)) {
          fs.unlinkSync(oldLogoPath);
        }
      }
    }

    const {
      school_name,
      school_short_name,
      motto,
      address,
      city,
      region,
      postal_code,
      phone_numbers,
      email,
      website,
      principal_name,
      registration_number,
      bank_name,
      branch_name,
      account_number,
      account_name,
      swift_code,
      mobile_money_provider,
      mobile_money_number,
      currency_symbol,
      receipt_footer,
      bill_terms,
      late_fee_percentage,
      updated_by,
    } = req.body;

    // Check if settings exist
    const [existingSettings] = await connection.query(
      "SELECT id FROM school_settings ORDER BY id DESC LIMIT 1",
    );

    let query;
    let values;

    if (existingSettings.length > 0) {
      const settingId = existingSettings[0].id;

      if (logo_filename) {
        // Update with logo
        query = `
          UPDATE school_settings SET 
            school_name = ?, school_short_name = ?, motto = ?, logo_filename = ?,
            address = ?, city = ?, region = ?, postal_code = ?,
            phone_numbers = ?, email = ?, website = ?,
            principal_name = ?, registration_number = ?,
            bank_name = ?, branch_name = ?, account_number = ?, 
            account_name = ?, swift_code = ?,
            mobile_money_provider = ?, mobile_money_number = ?,
            currency_symbol = ?, receipt_footer = ?, bill_terms = ?,
            late_fee_percentage = ?, updated_by = ?
          WHERE id = ?
        `;
        values = [
          school_name,
          school_short_name,
          motto,
          logo_filename,
          address,
          city,
          region,
          postal_code,
          phone_numbers,
          email,
          website,
          principal_name,
          registration_number,
          bank_name,
          branch_name,
          account_number,
          account_name,
          swift_code || null, // Handle null swift code
          mobile_money_provider || null,
          mobile_money_number || null,
          currency_symbol,
          receipt_footer,
          bill_terms || null,
          parseFloat(late_fee_percentage) || 0.0,
          parseInt(updated_by),
          settingId,
        ];
      } else {
        // Update without changing logo
        query = `
          UPDATE school_settings SET 
            school_name = ?, school_short_name = ?, motto = ?,
            address = ?, city = ?, region = ?, postal_code = ?,
            phone_numbers = ?, email = ?, website = ?,
            principal_name = ?, registration_number = ?,
            bank_name = ?, branch_name = ?, account_number = ?, 
            account_name = ?, swift_code = ?,
            mobile_money_provider = ?, mobile_money_number = ?,
            currency_symbol = ?, receipt_footer = ?, bill_terms = ?,
            late_fee_percentage = ?, updated_by = ?
          WHERE id = ?
        `;
        values = [
          school_name,
          school_short_name,
          motto,
          address,
          city,
          region,
          postal_code,
          phone_numbers,
          email,
          website,
          principal_name,
          registration_number,
          bank_name,
          branch_name,
          account_number,
          account_name,
          swift_code || null,
          mobile_money_provider || null,
          mobile_money_number || null,
          currency_symbol,
          receipt_footer,
          bill_terms || null,
          parseFloat(late_fee_percentage) || 0.0,
          parseInt(updated_by),
          settingId,
        ];
      }
    } else {
      // Create new settings
      query = `
        INSERT INTO school_settings (
          school_name, school_short_name, motto, logo_filename,
          address, city, region, postal_code,
          phone_numbers, email, website,
          principal_name, registration_number,
          bank_name, branch_name, account_number, 
          account_name, swift_code,
          mobile_money_provider, mobile_money_number,
          currency_symbol, receipt_footer, bill_terms,
          late_fee_percentage, updated_by
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `;
      values = [
        school_name,
        school_short_name,
        motto,
        logo_filename,
        address,
        city,
        region,
        postal_code,
        phone_numbers,
        email,
        website,
        principal_name,
        registration_number,
        bank_name,
        branch_name,
        account_number,
        account_name,
        swift_code || null,
        mobile_money_provider || null,
        mobile_money_number || null,
        currency_symbol,
        receipt_footer,
        bill_terms || null,
        parseFloat(late_fee_percentage) || 0.0,
        parseInt(updated_by),
      ];
    }

    // Execute query
    await connection.query(query, values);

    await connection.commit();
    clearRelevantCaches("UPDATE_SCHOOL_SETTINGS");
    // Return updated settings
    const [updated] = await connection.query(
      `
      SELECT ss.*, u.username as updated_by_name
      FROM school_settings ss
      LEFT JOIN users u ON ss.updated_by = u.id
      WHERE ss.id = ?
    `,
      [existingSettings[0]?.id || 1],
    );

    res.json(updated[0]);
  } catch (error) {
    await connection.rollback();
    console.error("Error updating school settings:", error);
    res.status(500).json({
      error: "Failed to update school settings",
      details: error.message,
    });
  } finally {
    connection.release();
  }
};

// Helper: Get school settings for PDF generation
const getSchoolSettingsForPDF = async () => {
  const cacheKey = "school_settings_pdf";

  // CHECK CACHE FIRST
  const cachedSettings = cache.get(cacheKey);
  if (cachedSettings) {
    console.log("📦 [CACHE HIT] School settings from cache");
    return cachedSettings;
  }

  console.log("🔄 [CACHE MISS] Fetching school settings from database");

  try {
    const [settings] = await pool.query(`
      SELECT * FROM school_settings 
      ORDER BY id DESC 
      LIMIT 1
    `);

    let result;
    if (settings.length === 0) {
      result = getDefaultSchoolSettings();
    } else {
      const setting = settings[0];

      // Parse phone numbers if it's a JSON string
      let phoneNumbers = ["(233) 123-4567"];
      if (setting.phone_numbers) {
        try {
          phoneNumbers =
            typeof setting.phone_numbers === "string"
              ? JSON.parse(setting.phone_numbers)
              : setting.phone_numbers;
        } catch (e) {
          phoneNumbers = [setting.phone_numbers];
        }
      }

      result = {
        school_name: setting.school_name || "School Manager Academy",
        school_short_name: setting.school_short_name,
        motto: setting.motto,
        address: setting.address || "123 Education Street, Learning City",
        city: setting.city,
        region: setting.region,
        postal_code: setting.postal_code,
        phone_numbers: phoneNumbers,
        email: setting.email || "info@sma.edu.gh",
        website: setting.website,
        principal_name: setting.principal_name,
        registration_number: setting.registration_number,
        bank_name: setting.bank_name,
        branch_name: setting.branch_name,
        account_number: setting.account_number,
        account_name: setting.account_name,
        swift_code: setting.swift_code,
        mobile_money_provider: setting.mobile_money_provider,
        mobile_money_number: setting.mobile_money_number,
        logo_filename: setting.logo_filename,
        currency_symbol: setting.currency_symbol || "Ghc",
        receipt_footer: setting.receipt_footer,
        bill_terms: setting.bill_terms,
        late_fee_percentage: setting.late_fee_percentage || 0.0,
      };
    }

    // SAVE TO CACHE (5 minutes)
    cache.set(cacheKey, result, 300);
    console.log("💾 [CACHE SAVED] School settings cached for 5 minutes");

    return result;
  } catch (error) {
    console.error("Error getting school settings for PDF:", error);
    return getDefaultSchoolSettings();
  }
};

// Helper for default settings
const getDefaultSchoolSettings = () => {
  return {
    school_name: "School Manager Academy",
    address: "123 Education Street, Learning City",
    phone_numbers: ["(233) 123-4567"],
    email: "info@sma.edu.gh",
    currency_symbol: "Ghc",
    motto: "Quality Education for All",
  };
};

const addSchoolLogoToPDF = async (doc, x, y, width, height) => {
  try {
    const schoolSettings = await getSchoolSettingsForPDF();

    if (schoolSettings.logo_filename) {
      const logoPath = path.join(
        __dirname,
        "../uploads/school-logo",
        schoolSettings.logo_filename,
      );

      if (fs.existsSync(logoPath)) {
        const logoBuffer = fs.readFileSync(logoPath);

        const base64 = logoBuffer.toString("base64");

        // Determine image type
        const ext = path.extname(schoolSettings.logo_filename).toLowerCase();
        const mimeTypes = {
          ".jpg": "image/jpeg",
          ".jpeg": "image/jpeg",
          ".png": "image/png",
          ".gif": "image/gif",
          ".webp": "image/webp",
        };

        const mimeType = mimeTypes[ext] || "image/jpeg";

        const dataUrl = `data:${mimeType};base64,${base64}`;

        // Try adding the image
        try {
          doc.addImage({
            imageData: dataUrl,
            x: x,
            y: y,
            width: width,
            height: height,
          });
          return true;
        } catch (imageError) {
          console.error("Error adding image to PDF:", imageError.message);
          throw imageError;
        }
      } else {
        console.error("Logo file not found at path:", logoPath);

        // List files in directory to debug
        const logoDir = path.join(__dirname, "../uploads/school-logo");
        if (fs.existsSync(logoDir)) {
          const files = fs.readdirSync(logoDir);
        } else {
          // console.log("Directory does not exist:", logoDir);
        }
      }
    } else {
      // console.log("No logo_filename in school settings");
    }

    // Fallback: School initials
    doc.setFillColor(240, 240, 240);
    doc.rect(x, y, width, height, "F");
    doc.setDrawColor(200, 200, 200);
    doc.setLineWidth(0.5);
    doc.rect(x, y, width, height);

    const schoolName = schoolSettings.school_name || "School";
    const initials = schoolName
      .split(" ")
      .map((word) => word[0])
      .join("")
      .substring(0, 2)
      .toUpperCase();

    doc.setTextColor(150, 150, 150);
    doc.setFontSize(14);
    doc.text(initials, x + width / 2, y + height / 2, { align: "center" });

    return false;
  } catch (error) {
    console.error("Error in addSchoolLogoToPDF:", error);
    console.error(error.stack);
    return false;
  }
};

// ADD THIS FUNCTION FOR CACHE MONITORING
const getCacheStats = (req, res) => {
  try {
    const stats = cache.getStats();
    const keys = cache.keys();

    console.log("Cache Stats Requested:", {
      success: true,
      cache_stats: {
        hits: stats.hits,
        misses: stats.misses,
        keys: keys.length,
        sample_keys: keys.slice(0, 10), // Show first 10 keys
        key_size: stats.keys,
        cache_size: stats.vsize,
      },
      timestamp: new Date().toISOString(),
    });

    res.json({
      success: true,
      cache_stats: {
        hits: stats.hits,
        misses: stats.misses,
        keys: keys.length,
        sample_keys: keys.slice(0, 10), // Show first 10 keys
        key_size: stats.keys,
        cache_size: stats.vsize,
      },
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    console.error("Error getting cache stats:", error);
    res.status(500).json({ error: "Failed to get cache stats" });
  }
};

// ADD THIS FUNCTION TO CLEAR CACHE IF NEEDED
const clearCache = (req, res) => {
  try {
    const keys = cache.keys();
    cache.flushAll();

    res.json({
      success: true,
      message: `Cache cleared (${keys.length} keys removed)`,
      cleared_keys: keys,
    });
  } catch (error) {
    console.error("Error clearing cache:", error);
    res.status(500).json({ error: "Failed to clear cache" });
  }
};

//dashboard stats
const getDashboardStats = async (req, res) => {
  try {
    const today = new Date().toISOString().split("T")[0];
    const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000)
      .toISOString()
      .split("T")[0];

    // Get current academic year
    const [currentYear] = await pool.query(
      "SELECT id, year_label FROM academic_years WHERE is_current = TRUE LIMIT 1",
    );

    const currentYearId = currentYear[0]?.id;
    const currentYearLabel = currentYear[0]?.year_label || "Not set";

    // Get current term
    const [currentTerm] = await pool.query(
      `SELECT id, term_name FROM terms 
       WHERE academic_year_id = ? 
       AND start_date <= CURDATE() 
       AND end_date >= CURDATE() 
       LIMIT 1`,
      [currentYearId],
    );

    const currentTermId = currentTerm[0]?.id;
    const currentTermName = currentTerm[0]?.term_name || "Not set";

    // Fetch all stats in parallel - FIXED QUERIES
    const [
      [students],
      [teachers],
      [classes],
      [attendance],
      [recentPayments],
      [pendingBills],
      [activeStudents],
      [activeClasses],
    ] = await Promise.all([
      pool.query("SELECT COUNT(*) as count FROM students"),
      pool.query("SELECT COUNT(*) as count FROM teachers"),
      pool.query("SELECT COUNT(*) as count FROM classes"),
      pool.query(
        `
        SELECT 
          COUNT(*) as total,
          SUM(CASE WHEN status = 'Present' THEN 1 ELSE 0 END) as present_count,
          SUM(CASE WHEN status = 'Absent' THEN 1 ELSE 0 END) as absent_count
        FROM attendance 
        WHERE date = ?
      `,
        [today],
      ),
      pool.query(`
        SELECT 
          COUNT(*) as count,
          SUM(amount_paid) as total_amount
        FROM payments 
        WHERE DATE(payment_date) = CURDATE()
      `),
      pool.query(`
        SELECT 
          COUNT(*) as count,
          SUM(remaining_amount) as total_amount
        FROM bills 
        WHERE payment_status IN ('Pending', 'Partially Paid')
          AND remaining_amount > 0
      `),
      pool.query(`
        SELECT COUNT(*) as count FROM students 
        WHERE (is_active IS NULL OR is_active = TRUE)
      `),
      pool.query(
        `
        SELECT COUNT(DISTINCT ca.class_id) as count 
        FROM class_assignments ca
        WHERE ca.academic_year_id = ?
      `,
        [currentYearId],
      ),
    ]);

    // Get total fees collected for current term
    const [termFees] = await pool.query(
      `
      SELECT 
        COALESCE(SUM(stb.total_amount), 0) as total_billed,
        COALESCE(SUM(stb.paid_amount), 0) as total_paid,
        COALESCE(SUM(stb.remaining_balance), 0) as total_pending
      FROM student_term_bills stb
      WHERE stb.academic_year_id = ? 
        AND stb.term_id = ?
        AND stb.is_finalized = TRUE
    `,
      [currentYearId, currentTermId],
    );

    // FIXED: Get recent activities - ALL IN ONE QUERY for proper sorting
    const [recentActivities] = await pool.query(`
      (SELECT 
        'payment' as type,
        CONCAT('Payment received - Ghc ', FORMAT(p.amount_paid, 2)) as description,
        p.payment_date as activity_date,
        CONCAT(s.first_name, ' ', s.last_name) as student_name,
        s.admission_number,
        p.amount_paid as amount,
        u.username as action_by,
        p.payment_date as timestamp,
        p.id as reference_id,
        1 as sort_order
      FROM payments p
      JOIN students s ON p.student_id = s.id
      JOIN users u ON p.received_by = u.id
      WHERE p.payment_date >= DATE_SUB(NOW(), INTERVAL 7 DAY)
      ORDER BY p.payment_date DESC
      LIMIT 4)
      
      UNION ALL
      
      (SELECT 
        'attendance' as type,
        CONCAT('Attendance recorded for ', COUNT(DISTINCT a.student_id), ' students') as description,
        a.date as activity_date,
        NULL as student_name,
        NULL as admission_number,
        COUNT(DISTINCT a.student_id) as amount,
        u.username as action_by,
        MAX(a.date) as timestamp,
        NULL as reference_id,
        2 as sort_order
      FROM attendance a
      JOIN users u ON a.recorded_by = u.id
      WHERE a.date >= DATE_SUB(NOW(), INTERVAL 7 DAY)
      GROUP BY a.date, u.username
      ORDER BY MAX(a.date) DESC
      LIMIT 3)
      
      UNION ALL
      
      (SELECT 
        'expense' as type,
        CONCAT('Expense: ', e.expense_category, ' - Ghc ', FORMAT(e.amount, 2)) as description,
        e.expense_date as activity_date,
        e.paid_to as student_name,
        NULL as admission_number,
        e.amount as amount,
        u.username as action_by,
        e.created_at as timestamp,
        e.id as reference_id,
        3 as sort_order
      FROM expenses e
      JOIN users u ON e.recorded_by = u.id
      WHERE e.expense_date >= DATE_SUB(NOW(), INTERVAL 7 DAY)
      ORDER BY e.created_at DESC
      LIMIT 3)
      
      UNION ALL
      
      (SELECT 
        'student' as type,
        CONCAT('New student enrolled: ', s.first_name, ' ', s.last_name) as description,
        s.enrolled_date as activity_date,
        CONCAT(s.first_name, ' ', s.last_name) as student_name,
        s.admission_number,
        NULL as amount,
        'System' as action_by,
        s.created_at as timestamp,
        s.id as reference_id,
        4 as sort_order
      FROM students s
      WHERE s.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
      ORDER BY s.created_at DESC
      LIMIT 3)
      
      UNION ALL
      
      (SELECT 
        'cash_receipt' as type,
        CONCAT('Cash receipt: ', cr.description, ' - Ghc ', FORMAT(cr.amount, 2)) as description,
        cr.receipt_date as activity_date,
        cr.source as student_name,
        NULL as admission_number,
        cr.amount as amount,
        u.username as action_by,
        cr.created_at as timestamp,
        cr.id as reference_id,
        5 as sort_order
      FROM daily_cash_receipts cr
      JOIN users u ON cr.received_by = u.id
      WHERE cr.receipt_date >= DATE_SUB(NOW(), INTERVAL 7 DAY)
      ORDER BY cr.created_at DESC
      LIMIT 3)
      
      ORDER BY timestamp DESC
      LIMIT 8
    `);

    // Get attendance percentage
    const attendanceTotal = attendance[0].total || 0;
    const attendancePresent = attendance[0].present_count || 0;
    const attendanceRate =
      attendanceTotal > 0
        ? Math.round((attendancePresent / attendanceTotal) * 100)
        : 0;

    // Get yesterday's attendance for comparison
    const [yesterdayAttendance] = await pool.query(
      `
      SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN status = 'Present' THEN 1 ELSE 0 END) as present_count
      FROM attendance 
      WHERE date = ?
    `,
      [yesterday],
    );

    const yesterdayTotal = yesterdayAttendance[0].total || 0;
    const yesterdayPresent = yesterdayAttendance[0].present_count || 0;
    const yesterdayRate =
      yesterdayTotal > 0
        ? Math.round((yesterdayPresent / yesterdayTotal) * 100)
        : 0;

    const attendanceChange = attendanceRate - yesterdayRate;

    // Get yesterday's payments for comparison
    const [yesterdayPayments] = await pool.query(`
      SELECT 
        SUM(amount_paid) as total_amount
      FROM payments 
      WHERE DATE(payment_date) = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
    `);

    const yesterdayPaymentAmount = yesterdayPayments[0]?.total_amount || 0;
    const todayPaymentAmount = recentPayments[0]?.total_amount || 0;
    const paymentChange =
      yesterdayPaymentAmount > 0
        ? Math.round(
            ((todayPaymentAmount - yesterdayPaymentAmount) /
              yesterdayPaymentAmount) *
              100,
          )
        : todayPaymentAmount > 0
          ? 100
          : 0;

    res.json({
      overview: {
        totalStudents: students[0].count,
        activeStudents: activeStudents[0].count,
        totalTeachers: teachers[0].count,
        totalClasses: classes[0].count,
        activeClasses: activeClasses[0].count,
        academicYear: currentYearLabel,
        currentTerm: currentTermName,
      },
      today: {
        attendance: {
          present: attendancePresent,
          absent: attendance[0].absent_count || 0,
          total: attendanceTotal,
          rate: attendanceRate,
          change: attendanceChange,
          trend:
            attendanceChange > 0
              ? "up"
              : attendanceChange < 0
                ? "down"
                : "same",
        },
        payments: {
          count: recentPayments[0].count,
          amount: todayPaymentAmount,
          change: paymentChange,
          trend: paymentChange > 0 ? "up" : paymentChange < 0 ? "down" : "same",
        },
      },
      finance: {
        termBilled: termFees[0]?.total_billed || 0,
        termPaid: termFees[0]?.total_paid || 0,
        termPending: termFees[0]?.total_pending || 0,
        pendingBills: {
          count: pendingBills[0].count,
          amount: pendingBills[0].total_amount || 0,
        },
        collectionRate:
          termFees[0]?.total_billed > 0
            ? Math.round(
                (termFees[0].total_paid / termFees[0].total_billed) * 100,
              )
            : 0,
      },
      recentActivities: recentActivities,
      statsTimestamp: new Date().toISOString(),
      cacheKey: `dashboard_stats_${today}_${currentYearId}_${currentTermId}`,
    });
  } catch (error) {
    console.error("Error fetching dashboard stats:", error);
    res.status(500).json({
      error: "Failed to fetch dashboard stats",
      details: error.message,
      stack: process.env.NODE_ENV === "development" ? error.stack : undefined,
    });
  }
};

// Send balance reminder email
const sendBalanceReminder = async (req, res) => {
  const connection = await pool.getConnection();

  try {
    const { student_id, academic_year_id, term_id } = req.body;

    if (!student_id) {
      return res.status(400).json({ error: "Student ID is required" });
    }

    // Get student details with current balance
    const [students] = await connection.query(
      `SELECT 
         s.id as student_id,
         s.first_name,
         s.last_name,
         s.admission_number,
         s.parent_name,
         s.parent_contact,
         s.parent_email,
         c.class_name,
         stb.total_amount,
         stb.paid_amount,
         stb.remaining_balance,
         stb.is_fully_paid,
         ay.year_label as academic_year,
         t.term_name
       FROM students s
       LEFT JOIN class_assignments ca ON s.id = ca.student_id 
         AND ca.academic_year_id = COALESCE(?, (SELECT id FROM academic_years WHERE is_current = TRUE))
       LEFT JOIN classes c ON ca.class_id = c.id
       LEFT JOIN student_term_bills stb ON s.id = stb.student_id
         AND stb.is_finalized = TRUE
         AND (stb.academic_year_id = COALESCE(?, ca.academic_year_id) OR stb.academic_year_id IS NULL)
         AND (stb.term_id = COALESCE(?, (SELECT id FROM terms ORDER BY start_date DESC LIMIT 1)) OR stb.term_id IS NULL)
       LEFT JOIN academic_years ay ON stb.academic_year_id = ay.id
       LEFT JOIN terms t ON stb.term_id = t.id
       WHERE s.id = ?
         AND (s.is_active IS NULL OR s.is_active = TRUE)`,
      [academic_year_id, academic_year_id, term_id, student_id],
    );

    const student = students[0];

    // Check if there's an email to send to
    if (!student.parent_email && !student.student_email) {
      return res.status(400).json({
        error:
          "No email address found for this student. Please add parent email first.",
        debug: {
          parent_email: student.parent_email,
          student_email: student.student_email,
          parent_contact: student.parent_contact,
        },
      });
    }

    // Check if there's actually a balance
    if (student.remaining_balance <= 0) {
      return res.status(400).json({
        error: "Student has no outstanding balance. No reminder needed.",
      });
    }

    // Get school settings
    const [settings] = await connection.query(
      "SELECT * FROM school_settings ORDER BY id DESC LIMIT 1",
    );

    let schoolSettings = {};
    if (settings.length > 0) {
      const setting = settings[0];
      let phoneNumbers = [];
      try {
        phoneNumbers = setting.phone_numbers
          ? JSON.parse(setting.phone_numbers)
          : [];
      } catch (e) {
        phoneNumbers = [setting.phone_numbers];
      }

      schoolSettings = {
        school_name: setting.school_name || "School Manager Academy",
        school_short_name: setting.school_short_name,
        motto: setting.motto,
        address: setting.address,
        city: setting.city,
        region: setting.region,
        phone_numbers: phoneNumbers,
        email: setting.email,
        website: setting.website,
        currency_symbol: setting.currency_symbol || "Ghc",
        bank_name: setting.bank_name,
        account_number: setting.account_number,
        account_name: setting.account_name,
        mobile_money_provider: setting.mobile_money_provider,
        mobile_money_number: setting.mobile_money_number,
      };
    } else {
      schoolSettings = {
        school_name: "School Manager Academy",
        currency_symbol: "Ghc",
      };
    }

    // Send the email
    const emailService = require("../utils/emailServices");

    const result = await emailService.sendBalanceReminder(
      student, // This is the student object
      {
        remaining_balance: student.remaining_balance,
        total_amount: student.total_amount,
        paid_amount: student.paid_amount,
        academic_year: student.academic_year,
        term_name: student.term_name,
      },
      schoolSettings,
    );
    if (result.success) {
      // Log the reminder in a table (optional - create a reminder_log table)
      try {
        await connection.query(
          `INSERT INTO email_logs 
           (student_id, email_type, recipient_email, sent_at, status, message_id) 
           VALUES (?, 'balance_reminder', ?, NOW(), 'sent', ?)`,
          [
            student_id,
            student.parent_email || student.student_email,
            result.messageId,
          ],
        );
      } catch (logError) {
        console.error("Error logging email:", logError);
        // Don't fail the request if logging fails
      }

      res.json({
        success: true,
        message: `Reminder sent successfully to ${student.parent_name || "parent"}`,
        recipient: student.parent_email || student.student_email,
        previewUrl: result.previewUrl,
      });
    } else {
      res.status(500).json({
        error: "Failed to send reminder email",
        details: result.error,
      });
    }
  } catch (error) {
    console.error("Error sending balance reminder:", error);
    res.status(500).json({
      error: "Failed to send balance reminder",
      details: error.message,
    });
  } finally {
    connection.release();
  }
};

//  Get email logs with proper pagination
const getEmailLogs = async (req, res) => {
  try {
    const {
      student_id,
      email_type,
      start_date,
      end_date,
      status,
      page = 1,
      limit = 50,
    } = req.query;

    // Validate pagination params
    const pageNum = parseInt(page);
    const limitNum = parseInt(limit);
    const offset = (pageNum - 1) * limitNum;

    // Build WHERE conditions
    let whereConditions = ["1=1"];
    let queryParams = [];

    // Student filter
    if (student_id) {
      whereConditions.push("el.student_id = ?");
      queryParams.push(student_id);
    }

    // Email type filter
    if (email_type) {
      whereConditions.push("el.email_type = ?");
      queryParams.push(email_type);
    }

    // Date range filter
    if (start_date && end_date) {
      whereConditions.push("DATE(el.sent_at) BETWEEN ? AND ?");
      queryParams.push(start_date, end_date);
    } else if (start_date) {
      whereConditions.push("DATE(el.sent_at) >= ?");
      queryParams.push(start_date);
    } else if (end_date) {
      whereConditions.push("DATE(el.sent_at) <= ?");
      queryParams.push(end_date);
    }

    // Status filter
    if (status) {
      whereConditions.push("el.status = ?");
      queryParams.push(status);
    }

    // Get TOTAL count for pagination (separate query)
    const [countResult] = await pool.query(
      `SELECT COUNT(*) as total 
       FROM email_logs el
       WHERE ${whereConditions.join(" AND ")}`,
      queryParams,
    );

    const total = countResult[0].total;
    const totalPages = Math.ceil(total / limitNum);

    // Get PAGINATED logs with student info
    const [logs] = await pool.query(
      `SELECT 
         el.*,
         CONCAT(s.first_name, ' ', s.last_name) as student_name,
         s.admission_number
       FROM email_logs el
       LEFT JOIN students s ON el.student_id = s.id
       WHERE ${whereConditions.join(" AND ")}
       ORDER BY el.sent_at DESC
       LIMIT ? OFFSET ?`,
      [...queryParams, limitNum, offset],
    );

    // Return paginated response
    res.json({
      success: true,
      data: logs,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total,
        totalPages,
        hasNextPage: pageNum < totalPages,
        hasPrevPage: pageNum > 1,
      },
      filters: {
        student_id: student_id || null,
        email_type: email_type || null,
        start_date: start_date || null,
        end_date: end_date || null,
        status: status || null,
      },
    });
  } catch (error) {
    console.error("Error fetching email logs:", error);
    res.status(500).json({
      success: false,
      error: "Failed to fetch email logs",
    });
  }
};

// Get email statistics (still works without pagination)
const getEmailStats = async (req, res) => {
  try {
    const { start_date, end_date } = req.query;

    let dateCondition = "1=1";
    let params = [];

    if (start_date && end_date) {
      dateCondition = "DATE(sent_at) BETWEEN ? AND ?";
      params.push(start_date, end_date);
    } else if (start_date) {
      dateCondition = "DATE(sent_at) >= ?";
      params.push(start_date);
    } else if (end_date) {
      dateCondition = "DATE(sent_at) <= ?";
      params.push(end_date);
    }

    // Overall stats
    const [overallStats] = await pool.query(
      `SELECT 
         COUNT(*) as total_emails,
         SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END) as successful,
         SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
         COUNT(DISTINCT student_id) as unique_students
       FROM email_logs
       WHERE ${dateCondition}`,
      params,
    );

    // Breakdown by email type
    const [typeBreakdown] = await pool.query(
      `SELECT 
         email_type,
         COUNT(*) as count,
         SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END) as sent_count,
         SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_count
       FROM email_logs
       WHERE ${dateCondition}
       GROUP BY email_type
       ORDER BY count DESC`,
      params,
    );

    // Daily activity for charts
    const [dailyActivity] = await pool.query(
      `SELECT 
         DATE(sent_at) as date,
         COUNT(*) as total,
         SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END) as sent,
         SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
       FROM email_logs
       WHERE ${dateCondition}
       GROUP BY DATE(sent_at)
       ORDER BY date DESC
       LIMIT 30`,
      params,
    );

    res.json({
      success: true,
      data: {
        overview: overallStats[0] || {
          total_emails: 0,
          successful: 0,
          failed: 0,
          unique_students: 0,
        },
        by_type: typeBreakdown,
        daily: dailyActivity,
      },
    });
  } catch (error) {
    console.error("Error fetching email stats:", error);
    res.status(500).json({
      success: false,
      error: "Failed to fetch email statistics",
    });
  }
};

// Send bulk balance reminders to all students with outstanding balances
const sendBulkBalanceReminders = async (req, res) => {
  const connection = await pool.getConnection();

  try {
    const { academic_year_id, term_id, filters = {} } = req.body;
    const userId = req.user?.id || null;

    // Get current academic year if not specified
    let yearId = academic_year_id;
    if (!yearId) {
      const [currentYear] = await connection.query(
        "SELECT id FROM academic_years WHERE is_current = TRUE LIMIT 1",
      );
      yearId = currentYear[0]?.id;
    }

    // Get current term if not specified
    let termId = term_id;
    if (!termId) {
      const [currentTerm] = await connection.query(
        `SELECT id FROM terms 
         WHERE academic_year_id = ? 
         AND start_date <= CURDATE() 
         AND end_date >= CURDATE() 
         LIMIT 1`,
        [yearId],
      );
      termId = currentTerm[0]?.id;
    }

    // Get all students with outstanding balances
    let query = `
      SELECT 
        s.id as student_id,
        s.first_name,
        s.last_name,
        s.admission_number,
        s.parent_name,
        s.parent_contact,
        s.parent_email,
        c.class_name,
        stb.total_amount,
        stb.paid_amount,
        stb.remaining_balance,
        ay.year_label as academic_year,
        t.term_name
      FROM students s
      INNER JOIN class_assignments ca ON s.id = ca.student_id 
        AND ca.academic_year_id = ?
      INNER JOIN classes c ON ca.class_id = c.id
      INNER JOIN student_term_bills stb ON s.id = stb.student_id
        AND stb.academic_year_id = ca.academic_year_id
        AND stb.term_id = ?
        AND stb.is_finalized = TRUE
        AND stb.remaining_balance > 0
      LEFT JOIN academic_years ay ON stb.academic_year_id = ay.id
      LEFT JOIN terms t ON stb.term_id = t.id
      WHERE (s.is_active IS NULL OR s.is_active = TRUE)
    `;

    const queryParams = [yearId, termId];

    // Add class filter if specified
    if (filters.class_id) {
      query += ` AND ca.class_id = ?`;
      queryParams.push(filters.class_id);
    }

    // Add minimum balance filter
    if (filters.min_balance) {
      query += ` AND stb.remaining_balance >= ?`;
      queryParams.push(filters.min_balance);
    }

    query += ` ORDER BY c.class_name, s.first_name`;

    const [students] = await connection.query(query, queryParams);

    if (students.length === 0) {
      return res.json({
        success: true,
        message: "No students with outstanding balances found",
        total_students: 0,
        sent_count: 0,
        failed_count: 0,
        results: [],
      });
    }

    // Get school settings
    const [settings] = await connection.query(
      "SELECT * FROM school_settings ORDER BY id DESC LIMIT 1",
    );

    let schoolSettings = {};
    if (settings.length > 0) {
      const setting = settings[0];
      let phoneNumbers = [];
      try {
        phoneNumbers = setting.phone_numbers
          ? JSON.parse(setting.phone_numbers)
          : [];
      } catch (e) {
        phoneNumbers = [setting.phone_numbers];
      }

      schoolSettings = {
        school_name: setting.school_name || "School Manager Academy",
        school_short_name: setting.school_short_name,
        motto: setting.motto,
        address: setting.address,
        city: setting.city,
        region: setting.region,
        phone_numbers: phoneNumbers,
        email: setting.email,
        website: setting.website,
        currency_symbol: setting.currency_symbol || "Ghc",
        bank_name: setting.bank_name,
        account_number: setting.account_number,
        account_name: setting.account_name,
        mobile_money_provider: setting.mobile_money_provider,
        mobile_money_number: setting.mobile_money_number,
      };
    } else {
      schoolSettings = {
        school_name: "School Manager Academy",
        currency_symbol: "Ghc",
      };
    }

    const emailService = require("../utils/emailServices");
    const results = [];

    // Send emails to all eligible students
    for (const student of students) {
      // Check if student has email
      const recipientEmail = student.parent_email || student.student_email;

      if (!recipientEmail) {
        results.push({
          student_id: student.student_id,
          name: `${student.first_name} ${student.last_name}`,
          success: false,
          message: "No email address",
          skipped: true,
        });
        continue;
      }

      try {
        // Send reminder
        const result = await emailService.sendBalanceReminder(
          student,
          {
            remaining_balance: student.remaining_balance,
            total_amount: student.total_amount,
            paid_amount: student.paid_amount,
            academic_year: student.academic_year,
            term_name: student.term_name,
          },
          schoolSettings,
        );

        if (result.success) {
          // Log success
          await connection.query(
            `INSERT INTO email_logs 
             (student_id, email_type, recipient_email, status, message_id, sent_at) 
             VALUES (?, 'balance_reminder', ?, 'sent', ?, NOW())`,
            [student.student_id, recipientEmail, result.messageId],
          );

          results.push({
            student_id: student.student_id,
            name: `${student.first_name} ${student.last_name}`,
            success: true,
            recipient: recipientEmail,
            messageId: result.messageId,
          });
        } else {
          // Log failure
          await connection.query(
            `INSERT INTO email_logs 
             (student_id, email_type, recipient_email, status, error_message, sent_at) 
             VALUES (?, 'balance_reminder', ?, 'failed', ?, NOW())`,
            [
              student.student_id,
              recipientEmail,
              result.message || "Unknown error",
            ],
          );

          results.push({
            student_id: student.student_id,
            name: `${student.first_name} ${student.last_name}`,
            success: false,
            recipient: recipientEmail,
            message: result.message,
          });
        }
      } catch (error) {
        // Log exception
        await connection.query(
          `INSERT INTO email_logs 
           (student_id, email_type, recipient_email, status, error_message, sent_at) 
           VALUES (?, 'balance_reminder', ?, 'failed', ?, NOW())`,
          [student.student_id, recipientEmail, error.message],
        );

        results.push({
          student_id: student.student_id,
          name: `${student.first_name} ${student.last_name}`,
          success: false,
          recipient: recipientEmail,
          message: error.message,
          error: true,
        });
      }

      // Small delay to avoid overwhelming email server
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    // Calculate summary
    const sentCount = results.filter((r) => r.success).length;
    const failedCount = results.filter((r) => !r.success && !r.skipped).length;
    const skippedCount = results.filter((r) => r.skipped).length;

    res.json({
      success: true,
      message: `Bulk reminders processed: ${sentCount} sent, ${failedCount} failed, ${skippedCount} skipped`,
      total_students: students.length,
      sent_count: sentCount,
      failed_count: failedCount,
      skipped_count: skippedCount,
      results: results,
      summary: {
        academic_year: students[0]?.academic_year || "Current",
        term: students[0]?.term_name || "Current",
        total_outstanding: students
          .reduce((sum, s) => sum + parseFloat(s.remaining_balance), 0)
          .toFixed(2),
      },
    });
  } catch (error) {
    console.error("Error sending bulk reminders:", error);
    res.status(500).json({
      success: false,
      error: "Failed to send bulk reminders",
      details: error.message,
    });
  } finally {
    connection.release();
  }
};

module.exports = {
  getUsers,
  getSubjects,
  createNewSubject,
  updateSubjects,
  deleteSubject,
  getTeachers,
  createTeacher,
  updateTeachers,
  deleteTeacher,

  getAcademicYears,
  getAcademicYearsPaginated,
  createAcademicYear,
  updateAcademicYear,
  setCurrentYear,
  deleteAcademicYear,
  getTerms,
  getTermsByAcademicYear,
  createTerm,
  updateTerm,
  deleteTerm,

  getClasses,
  getClassesPaginated,
  createClass,
  updateClass,
  deleteClass,
  getClassWithStudents,
  exportClassStudents,

  getClassTeachers,
  getClassTeacherById,
  assignClassTeacher,
  updateClassTeacher,
  deleteClassTeacher,
  getAvailableTeachers,

  getSubjectAssignments,
  createSubjectAssignment,
  updateSubjectAssignment,
  deleteSubjectAssignment,
  getAcademicYearsForSujectAssignment,

  getClassAssignments,
  createClassAssignment,
  createBulkClassAssignments,
  updateClassAssignment,
  promoteStudent,
  deleteClassAssignment,

  getStudents,
  createStudent,
  updateStudent,
  deactivateStudent,
  activateStudent,
  importStudents,
  exportStudents,

  getGradingScales,
  getGradingScaleById,
  createGradingScale,
  updateGradingScale,
  deleteGradingScale,
  calculateGrade,

  createBulkGrades,
  getClassSubjects,
  getGrades,
  exportGradeTemplate,
  importGrades,

  getReportCards,
  getReportCardById,
  generateReportCards,
  getIndividualReportCardPDF,
  updateReportCard,
  generateClassReportCardsPDF,
  generateStudentReportCardPDF,

  //attendance controllers
  getStudentsForAttendance,
  markAttendance,
  markBulkAttendance,
  getAttendanceRecords,
  getAttendanceStatistics,
  getAttendanceReports,
  exportAttendanceReport,

  //fee category controllers
  getFeeCategories,
  createFeeCategory,
  updateFeeCategory,
  deleteFeeCategory,

  //bill template
  getBillTemplates,
  createBillTemplate,
  updateBillTemplate,
  deleteBillTemplate,

  //student bill controllers
  getStudentBills,
  getStudentsWithPreviousBalances,
  generateBillsFromTemplates,
  saveStudentTermBill,
  getStudentTermBill,
  checkStudentPayments,
  getAvailableBillsForStudent,
  addBillsToFinalizedTerm,

  //student arrears controllers
  getStudentArrears,
  addStudentArrear,
  deleteStudentArrear,
  // deleteAllArrears,

  //overpayments controllers
  getStudentOverpayments,
  addStudentOverpayment,
  deleteStudentOverpayment,
  // deleteAllOverpayments,
  generateClassBillsPDF,

  //receipt controllers
  getStudentsByClass,
  processPayment,
  generateReceiptPDF,
  getPaymentHistory,
  getPaymentAllocations,

  //receipts controllers
  getAllReceipts,

  //financial records
  getPaymentsByCategory,
  getStudentStatements,
  getClassCollections,
  exportFinancialData,

  // Daily Cash Receipts
  recordCashReceipt,
  getCashReceipts,
  getCashReceiptsSummary,
  getCashReceiptsStats,
  exportCashReceipts,
  deleteCashReceipt,

  // Expenses Management
  getExpenses,
  getExpenseById,
  createExpense,
  updateExpense,
  deleteExpense,
  getExpenseStatistics,
  getExpenseCategories,
  exportExpenses,

  // School Settings
  getSchoolSettings,
  updateSchoolSettings,
  getSchoolSettingsForPDF,

  // Cache Monitoring
  getCacheStats,
  clearCache,

  // Dashboard Stats
  getDashboardStats,
  sendBalanceReminder,
  sendBulkBalanceReminders,

  getEmailLogs,
  getEmailStats,
};
