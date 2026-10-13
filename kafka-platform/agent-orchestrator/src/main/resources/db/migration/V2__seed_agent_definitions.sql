INSERT INTO agent_definitions (agent_type, description, system_prompt, model_name, temperature, max_tokens, timeout_seconds, max_retry_attempts, version)
VALUES
    ('researcher', 'Deterministic mock researcher', 'Return deterministic research output.', 'mock-deterministic', 0.00, 1024, 60, 3, 1),
    ('planner', 'Deterministic mock planner', 'Return deterministic plan output.', 'mock-deterministic', 0.00, 1024, 60, 3, 1),
    ('coder', 'Deterministic mock coder', 'Return deterministic code output.', 'mock-deterministic', 0.00, 2048, 60, 3, 1),
    ('test_writer', 'Deterministic mock test writer', 'Return deterministic test output.', 'mock-deterministic', 0.00, 2048, 60, 3, 1),
    ('tester', 'Deterministic mock tester', 'Return deterministic test execution output.', 'mock-deterministic', 0.00, 2048, 60, 3, 1),
    ('reviewer', 'Deterministic mock reviewer', 'Return deterministic review output.', 'mock-deterministic', 0.00, 2048, 60, 3, 1),
    ('fixer', 'Deterministic mock fixer', 'Return deterministic fix output.', 'mock-deterministic', 0.00, 2048, 60, 3, 1),
    ('linter', 'Deterministic mock linter', 'Return deterministic lint output.', 'mock-deterministic', 0.00, 1024, 60, 3, 1),
    ('quality', 'Deterministic mock quality gate', 'Return deterministic quality output.', 'mock-deterministic', 0.00, 1024, 60, 3, 1),
    ('refiner', 'Deterministic mock refiner', 'Return deterministic refinement output.', 'mock-deterministic', 0.00, 2048, 60, 3, 1)
ON CONFLICT (agent_type) DO NOTHING;
