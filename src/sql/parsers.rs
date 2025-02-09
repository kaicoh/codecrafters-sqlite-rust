use nom::{
    bytes::complete::{tag, tag_no_case, take_while, take_while1},
    character::complete::{multispace0, multispace1},
    combinator::opt,
    multi::{separated_list0, separated_list1},
    sequence::{delimited, preceded},
    IResult, Parser,
};

type StrParser = dyn Fn(&str) -> IResult<&str, &str>;

type TableName<'a> = &'a str;
type ColName<'a> = &'a str;
type ColType<'a> = &'a str;
type ColDef<'a> = (ColName<'a>, ColType<'a>);
type Condition<'a> = (ColName<'a>, &'a str);

pub fn parse_select(
    input: &str,
) -> IResult<&str, (Vec<ColName<'_>>, TableName<'_>, Vec<Condition<'_>>)> {
    let (remaining, columns) = delimited(
        parse_keyword("select"),
        parse_comma_separated_identifiers,
        parse_keyword("from"),
    )
    .parse(input)?;
    let (remaining, table) = parse_table_name(remaining)?;

    let (remaining, r#where) = opt(parse_keyword("where")).parse(remaining)?;
    if r#where.is_none() {
        Ok((remaining, (columns, table, vec![])))
    } else {
        let (remaining, conditions) = parse_and_conditions(remaining)?;
        Ok((remaining, (columns, table, conditions)))
    }
}

pub fn parse_create_table(input: &str) -> IResult<&str, (Vec<ColDef<'_>>, TableName<'_>)> {
    let (remaining, _) = parse_keyword("create").parse(input)?;
    let (remaining, _) = parse_keyword("table").parse(remaining)?;
    let (remaining, table) = parse_table_name(remaining)?;
    let (remaining, col_defs) = delimited(
        trim(tag("(")),
        parse_comma_separated_col_defs,
        trim(tag(")")),
    )
    .parse(remaining)?;
    Ok((remaining, (col_defs, table)))
}

fn parse_keyword(keyword: &'static str) -> Box<StrParser> {
    Box::new(move |input: &str| {
        delimited(multispace0, tag_no_case(keyword), multispace1).parse(input)
    })
}

fn parse_comma_separated_identifiers(input: &str) -> IResult<&str, Vec<&str>> {
    separated_list1(trim(tag(",")), trim(parse_identifier)).parse(input)
}

fn parse_comma_separated_col_defs(input: &str) -> IResult<&str, Vec<ColDef<'_>>> {
    separated_list1(trim(tag(",")), parse_col_defs).parse(input)
}

fn parse_and_conditions(input: &str) -> IResult<&str, Vec<Condition<'_>>> {
    separated_list0(trim(tag_no_case("and")), parse_eq_condition).parse(input)
}

fn trim<'a>(
    f: impl Parser<&'a str, Output = &'a str, Error = nom::error::Error<&'a str>>,
) -> impl Parser<&'a str, Output = &'a str, Error = nom::error::Error<&'a str>> {
    delimited(multispace0, f, multispace0)
}

fn parse_col_defs(input: &str) -> IResult<&str, ColDef<'_>> {
    let (remaining, col_name) = preceded(multispace0, parse_col_name_and_def).parse(input)?;
    let (remaining, col_type) = preceded(multispace0, parse_col_name_and_def).parse(remaining)?;
    let (remaining, _) = take_while(is_any_line_chars).parse(remaining)?;
    Ok((remaining, (col_name, col_type)))
}

fn parse_eq_condition(input: &str) -> IResult<&str, Condition<'_>> {
    let (remaining, col_name) = preceded(multispace0, parse_col_name_and_def).parse(input)?;
    let (remaining, _) = trim(tag("=")).parse(remaining)?;
    let (remaining, value) = preceded(multispace0, parse_any_value).parse(remaining)?;
    Ok((
        remaining,
        (col_name, value.trim_matches('\'').trim_matches('"')),
    ))
}

fn parse_identifier(input: &str) -> IResult<&str, &str> {
    take_while1(is_identifier_chars).parse(input)
}

fn parse_table_name(input: &str) -> IResult<&str, &str> {
    take_while1(is_table_name_chars).parse(input)
}

fn parse_col_name_and_def(input: &str) -> IResult<&str, &str> {
    take_while1(is_table_name_chars).parse(input)
}

fn parse_any_value(input: &str) -> IResult<&str, &str> {
    take_while1(is_any_value_chars).parse(input)
}

fn is_identifier_chars(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '*' || c == '(' || c == ')'
}

fn is_table_name_chars(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

fn is_any_line_chars(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c.is_ascii_whitespace()
}

fn is_any_value_chars(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '\'' || c == '"'
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    type TestResult = std::result::Result<(), Box<dyn Error>>;

    #[test]
    fn it_parses_and_separated_conditions() -> TestResult {
        let input = "foo = 'bar' and baz = \"foobarbaz\"";
        let (remaining, parsed) = parse_and_conditions(input)?;
        assert_eq!(remaining, "");
        assert_eq!(parsed, vec![("foo", "bar"), ("baz", "foobarbaz")]);
        Ok(())
    }

    #[test]
    fn it_parses_eq_condition() -> TestResult {
        let input = "foo = 'bar'";
        let (_, (col, val)) = parse_eq_condition(input)?;
        assert_eq!(col, "foo");
        assert_eq!(val, "bar");

        let input = "\nfoo = bar and ...";
        let (remaining, (col, val)) = parse_eq_condition(input)?;
        assert_eq!(remaining, " and ...");
        assert_eq!(col, "foo");
        assert_eq!(val, "bar");

        Ok(())
    }

    #[test]
    fn it_parses_create_table_sentences() -> TestResult {
        let input = "CREATE TABLE sqlite_schema(\n\
                type text,\n\
                name text,\n\
                tbl_name text,\n\
                rootpage integer,\n\
                sql text\n\
            );";
        let (_, (columns, table)) = parse_create_table(input)?;
        assert_eq!(
            columns,
            vec![
                ("type", "text"),
                ("name", "text"),
                ("tbl_name", "text"),
                ("rootpage", "integer"),
                ("sql", "text")
            ]
        );
        assert_eq!(table, "sqlite_schema");

        let input = "CREATE TABLE oranges\n\
            (\n\
                id integer primary key autoincrement,\n\
                name text,\n\
                description text\n\
            );";
        let (_, (columns, table)) = parse_create_table(input)?;
        assert_eq!(
            columns,
            vec![("id", "integer"), ("name", "text"), ("description", "text"),]
        );
        assert_eq!(table, "oranges");

        let input = "CREATE TABLE watermelon (id integer primary key, name text)";
        let (_, (columns, table)) = parse_create_table(input)?;
        assert_eq!(columns, vec![("id", "integer"), ("name", "text"),]);
        assert_eq!(table, "watermelon");
        Ok(())
    }

    #[test]
    fn it_parses_comma_separated_column_definitions() -> TestResult {
        let input = "\n\
                type text,\n\
                name text\n\
            ";
        let (remaining, parsed) = parse_comma_separated_col_defs(input)?;
        assert_eq!(remaining, "");
        assert_eq!(parsed, vec![("type", "text"), ("name", "text")]);

        let input = "\n tbl_name text,rootpage  integer";
        let (remaining, parsed) = parse_comma_separated_col_defs(input)?;
        assert_eq!(remaining, "");
        assert_eq!(parsed, vec![("tbl_name", "text"), ("rootpage", "integer")]);

        let input = "id integer primary key autoincrement,\n\tname  text";
        let (remaining, parsed) = parse_comma_separated_col_defs(input)?;
        assert_eq!(remaining, "");
        assert_eq!(parsed, vec![("id", "integer"), ("name", "text")]);

        Ok(())
    }

    #[test]
    fn it_parses_select_sentences() -> TestResult {
        let input = "SELECT name, producer FROM apples";
        let (_, (columns, table, _)) = parse_select(input)?;
        assert_eq!(columns, vec!["name", "producer"]);
        assert_eq!(table, "apples");

        let input = "SELECT * FROM oranges";
        let (_, (columns, table, _)) = parse_select(input)?;
        assert_eq!(columns, vec!["*"]);
        assert_eq!(table, "oranges");

        let input = "SELECT name, foo_bar FROM grapes";
        let (_, (columns, table, _)) = parse_select(input)?;
        assert_eq!(columns, vec!["name", "foo_bar"]);
        assert_eq!(table, "grapes");

        let input = "SELECT count(*) FROM grapes";
        let (_, (columns, table, _)) = parse_select(input)?;
        assert_eq!(columns, vec!["count(*)"]);
        assert_eq!(table, "grapes");

        let input = "SELECT name, color FROM apples WHERE color = 'Yellow'";
        let (_, (columns, table, conditions)) = parse_select(input)?;
        assert_eq!(columns, vec!["name", "color"]);
        assert_eq!(table, "apples");
        assert_eq!(conditions, vec![("color", "Yellow")]);

        Ok(())
    }

    #[test]
    fn it_parses_comma_separated_string() -> TestResult {
        let input = "foo,bar,baz";
        let (remaining, parsed) = parse_comma_separated_identifiers(input)?;
        assert_eq!(remaining, "");
        assert_eq!(parsed, vec!["foo", "bar", "baz"]);

        let input = " foo ,\nbar\n, baz ";
        let (remaining, parsed) = parse_comma_separated_identifiers(input)?;
        assert_eq!(remaining, "");
        assert_eq!(parsed, vec!["foo", "bar", "baz"]);

        let input = " foo ";
        let (remaining, parsed) = parse_comma_separated_identifiers(input)?;
        assert_eq!(remaining, "");
        assert_eq!(parsed, vec!["foo"]);

        let input = "*";
        let (remaining, parsed) = parse_comma_separated_identifiers(input)?;
        assert_eq!(remaining, "");
        assert_eq!(parsed, vec!["*"]);

        Ok(())
    }

    #[test]
    fn it_trims_string() -> TestResult {
        let mut parser = trim(parse_identifier);
        let input = "\n foo\t\r";
        let (remaining, parsed) = parser.parse(input)?;
        assert_eq!(remaining, "");
        assert_eq!(parsed, "foo");

        let input = "foo   ";
        let (remaining, parsed) = parser.parse(input)?;
        assert_eq!(remaining, "");
        assert_eq!(parsed, "foo");

        let input = " \nfoo?";
        let (remaining, parsed) = parser.parse(input)?;
        assert_eq!(remaining, "?");
        assert_eq!(parsed, "foo");

        Ok(())
    }

    #[test]
    fn it_parses_identifiers() -> TestResult {
        let input = "foobar baz";
        let (remaining, parsed) = parse_identifier(input)?;
        assert_eq!(remaining, " baz");
        assert_eq!(parsed, "foobar");

        let input = "* baz";
        let (remaining, parsed) = parse_identifier(input)?;
        assert_eq!(remaining, " baz");
        assert_eq!(parsed, "*");

        let input = "foo* baz";
        let (remaining, parsed) = parse_identifier(input)?;
        assert_eq!(remaining, " baz");
        assert_eq!(parsed, "foo*");

        let input = "foo_bar baz";
        let (remaining, parsed) = parse_identifier(input)?;
        assert_eq!(remaining, " baz");
        assert_eq!(parsed, "foo_bar");

        let input = "foo_bar";
        let (remaining, parsed) = parse_identifier(input)?;
        assert_eq!(remaining, "");
        assert_eq!(parsed, "foo_bar");

        Ok(())
    }

    #[test]
    fn it_parses_any_keyword() -> TestResult {
        let parse_select = parse_keyword("select");
        let input = " select\n  count(*)   \nfrom";
        let (remaining, parsed) = parse_select(input)?;
        assert_eq!(remaining, "count(*)   \nfrom");
        assert_eq!(parsed, "select");

        let input = "SELECT\n  COUNT(*)   \nFROM";
        let (remaining, parsed) = parse_select(input)?;
        assert_eq!(remaining, "COUNT(*)   \nFROM");
        assert_eq!(parsed, "SELECT");

        Ok(())
    }
}
