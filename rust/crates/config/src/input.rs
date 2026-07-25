use std::{
    fs::File,
    io::{self, Read},
    path::Path,
};

use serde::de::DeserializeOwned;

use crate::{ConfigError, ConfigResult};

pub(crate) const MAX_CONFIG_FILE_BYTES: usize = 1_048_576;

pub(crate) fn read_config_file(path: &Path) -> ConfigResult<String> {
    let file = File::open(path).map_err(|source| ConfigError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let metadata = file.metadata().map_err(|source| ConfigError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    if metadata.len() > MAX_CONFIG_FILE_BYTES as u64 {
        return Err(ConfigError::Validation(format!(
            "configuration {} has {} bytes; maximum is {MAX_CONFIG_FILE_BYTES}",
            path.display(),
            metadata.len()
        )));
    }

    let mut bytes = Vec::with_capacity(
        usize::try_from(metadata.len())
            .unwrap_or(MAX_CONFIG_FILE_BYTES)
            .min(MAX_CONFIG_FILE_BYTES)
            .saturating_add(1),
    );
    file.take(MAX_CONFIG_FILE_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| ConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    if bytes.len() > MAX_CONFIG_FILE_BYTES {
        return Err(ConfigError::Validation(format!(
            "configuration {} exceeded the {MAX_CONFIG_FILE_BYTES}-byte read limit",
            path.display()
        )));
    }

    String::from_utf8(bytes).map_err(|source| ConfigError::Io {
        path: path.to_path_buf(),
        source: io::Error::new(io::ErrorKind::InvalidData, source),
    })
}

/// Reads a configuration file with the shared bounded-input safety checks.
///
/// # Errors
///
/// Returns [`ConfigError`] when the file cannot be read as UTF-8 text, exceeds
/// the bounded size limit, or contains rejected YAML anchors or aliases.
pub fn read_bounded_config(path: impl AsRef<Path>) -> ConfigResult<String> {
    let yaml = read_config_file(path.as_ref())?;
    validate_bounded_yaml_text(&yaml)?;
    Ok(yaml)
}

pub(crate) fn parse_yaml<T: DeserializeOwned>(yaml: &str) -> ConfigResult<T> {
    let yaml = validate_bounded_yaml_text(yaml)?;
    Ok(serde_yaml::from_str(yaml)?)
}

fn validate_bounded_yaml_text(yaml: &str) -> ConfigResult<&str> {
    reject_oversized_config_input(yaml.len())?;
    let yaml = normalize_utf8_bom(yaml);
    reject_normalized_yaml_anchors_and_aliases(yaml)?;
    Ok(yaml)
}

fn reject_oversized_config_input(byte_len: usize) -> ConfigResult<()> {
    if byte_len > MAX_CONFIG_FILE_BYTES {
        return Err(ConfigError::Validation(format!(
            "configuration input has {byte_len} bytes; maximum is {MAX_CONFIG_FILE_BYTES}"
        )));
    }
    Ok(())
}

/// Rejects YAML anchors and aliases before deserialization.
///
/// Quoted text, comments, and literal or folded block-scalar content are not
/// interpreted as YAML control tokens. The scan uses constant-size lexical
/// state and resumes at the first non-empty line outside a block scalar's
/// indentation range.
///
/// # Errors
///
/// Returns [`ConfigError::Validation`] when an anchor or alias token is found.
pub fn reject_yaml_anchors_and_aliases(yaml: &str) -> ConfigResult<()> {
    reject_normalized_yaml_anchors_and_aliases(yaml)
}

fn normalize_utf8_bom(yaml: &str) -> &str {
    yaml.strip_prefix('\u{FEFF}').unwrap_or(yaml)
}

fn reject_normalized_yaml_anchors_and_aliases(yaml: &str) -> ConfigResult<()> {
    let mut state = YamlGuardState::default();
    for (line_index, line) in YamlLogicalLines::new(yaml).enumerate() {
        state.scan_line(line, line_index + 1)?;
    }
    Ok(())
}

const MAX_YAML_NESTING: usize = 128;

#[derive(Debug, Clone)]
struct YamlLogicalLines<'a> {
    remaining: &'a str,
}

impl<'a> YamlLogicalLines<'a> {
    const fn new(yaml: &'a str) -> Self {
        Self { remaining: yaml }
    }
}

impl<'a> Iterator for YamlLogicalLines<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining.is_empty() {
            return None;
        }
        for (byte_index, character) in self.remaining.char_indices() {
            let separator_bytes = match character {
                '\r' if self.remaining.as_bytes().get(byte_index + 1) == Some(&b'\n') => 2,
                '\n' | '\r' => 1,
                '\u{0085}' | '\u{2028}' | '\u{2029}' => character.len_utf8(),
                _ => continue,
            };
            let line = &self.remaining[..byte_index];
            self.remaining = &self.remaining[byte_index + separator_bytes..];
            return Some(line);
        }
        let line = self.remaining;
        self.remaining = "";
        Some(line)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GuardMode {
    ExpectNode,
    Plain,
    AfterNode,
    SingleQuoted,
    DoubleQuoted,
    Tag,
    VerbatimTag,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FlowFrame {
    closing: char,
    start_column: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BlockScalarState {
    parent_indentation: usize,
    content_indentation: Option<usize>,
}

type YamlCharacters<'a> = std::iter::Peekable<std::str::CharIndices<'a>>;

#[derive(Debug)]
struct YamlGuardState {
    mode: GuardMode,
    flow_stack: Vec<FlowFrame>,
    plain_parent_indentation: Option<usize>,
    plain_allows_equal_indentation: bool,
    plain_start_column: usize,
    node_start_column: usize,
    pending_parent_indentation: Option<usize>,
    node_property_start_column: Option<usize>,
    block_scalar: Option<BlockScalarState>,
}

impl Default for YamlGuardState {
    fn default() -> Self {
        Self {
            mode: GuardMode::ExpectNode,
            flow_stack: Vec::new(),
            plain_parent_indentation: None,
            plain_allows_equal_indentation: false,
            plain_start_column: 0,
            node_start_column: 0,
            pending_parent_indentation: None,
            node_property_start_column: None,
            block_scalar: None,
        }
    }
}

impl YamlGuardState {
    fn scan_line(&mut self, line: &str, line_number: usize) -> ConfigResult<()> {
        if self.consume_block_scalar_content(line) {
            return Ok(());
        }

        if is_blank_yaml_line(line) {
            return Ok(());
        }

        if self.mode != GuardMode::SingleQuoted
            && self.mode != GuardMode::DoubleQuoted
            && is_full_yaml_comment(line)
        {
            return Ok(());
        }

        let indentation = yaml_line_indentation(line);
        let mut line_parent_indentation = self.prepare_logical_line(line, indentation);
        if line.is_empty() {
            return self.finish_logical_line(line_number);
        }

        let mut characters = line.char_indices().peekable();
        let mut previous = None;
        let mut at_logical_column_zero = true;
        let mut next_column = 0_usize;

        while let Some((byte_index, character)) = characters.next() {
            if at_logical_column_zero
                && !matches!(
                    self.mode,
                    GuardMode::Plain | GuardMode::SingleQuoted | GuardMode::DoubleQuoted
                )
                && character == '\u{FEFF}'
            {
                next_column = next_column.saturating_add(1);
                continue;
            }
            let is_logical_column_zero = at_logical_column_zero;
            at_logical_column_zero = false;
            let column = next_column;
            next_column = next_column.saturating_add(1);

            if self.scan_quoted_character(character, &mut characters, &mut next_column) {
                previous = Some(character);
                continue;
            }
            if self.scan_tag_character(character, line_number)? {
                previous = Some(character);
                continue;
            }

            if character == '#'
                && previous.is_none_or(|value| is_yaml_blank(value) || is_flow_boundary(value))
            {
                if self.mode == GuardMode::Plain {
                    self.mode = GuardMode::AfterNode;
                    self.plain_parent_indentation = None;
                }
                break;
            }

            match self.mode {
                GuardMode::Plain => self.scan_plain_character(
                    character,
                    characters.peek().map(|(_, next)| *next),
                    column,
                    line_number,
                    &mut line_parent_indentation,
                )?,
                GuardMode::AfterNode => self.scan_after_node_character(
                    character,
                    column,
                    line_number,
                    &mut line_parent_indentation,
                )?,
                GuardMode::ExpectNode => {
                    if self.scan_expected_character(
                        line,
                        byte_index,
                        character,
                        column,
                        line_number,
                        &mut characters,
                        &mut next_column,
                        is_logical_column_zero,
                        &mut line_parent_indentation,
                    )? {
                        break;
                    }
                }
                GuardMode::SingleQuoted
                | GuardMode::DoubleQuoted
                | GuardMode::Tag
                | GuardMode::VerbatimTag => unreachable!(),
            }
            previous = Some(character);
        }

        self.finish_logical_line(line_number)
    }

    fn consume_block_scalar_content(&mut self, line: &str) -> bool {
        let Some(state) = self.block_scalar.as_mut() else {
            return false;
        };
        if is_blank_yaml_line(line) {
            return true;
        }
        let indentation = yaml_line_indentation(line);
        match state.content_indentation {
            Some(required) if indentation >= required => true,
            None if indentation > state.parent_indentation => {
                state.content_indentation = Some(indentation);
                true
            }
            _ => {
                self.block_scalar = None;
                self.mode = GuardMode::ExpectNode;
                false
            }
        }
    }

    fn prepare_logical_line(&mut self, line: &str, indentation: usize) -> usize {
        if !self.flow_stack.is_empty()
            || matches!(self.mode, GuardMode::SingleQuoted | GuardMode::DoubleQuoted)
        {
            return indentation;
        }
        if self.mode == GuardMode::Plain
            && self.plain_parent_indentation.is_some_and(|parent| {
                indentation > parent
                    || (self.plain_allows_equal_indentation
                        && indentation == parent
                        && !is_yaml_document_boundary_line(line))
            })
        {
            return self.plain_parent_indentation.unwrap_or(indentation);
        }
        if self
            .pending_parent_indentation
            .is_some_and(|parent| indentation > parent)
        {
            self.mode = GuardMode::ExpectNode;
            self.plain_parent_indentation = None;
            self.plain_allows_equal_indentation = false;
            self.node_property_start_column = None;
            return self.pending_parent_indentation.unwrap_or(indentation);
        }
        self.mode = GuardMode::ExpectNode;
        self.plain_parent_indentation = None;
        self.plain_allows_equal_indentation = false;
        self.pending_parent_indentation = None;
        self.node_property_start_column = None;
        indentation
    }

    fn finish_logical_line(&mut self, line_number: usize) -> ConfigResult<()> {
        if self.mode == GuardMode::VerbatimTag {
            return Err(ConfigError::Validation(format!(
                "unterminated YAML verbatim tag in bounded configuration input (line {line_number})"
            )));
        }
        if self.flow_stack.is_empty() {
            match self.mode {
                GuardMode::Plain | GuardMode::SingleQuoted | GuardMode::DoubleQuoted => {}
                GuardMode::ExpectNode
                | GuardMode::AfterNode
                | GuardMode::Tag
                | GuardMode::VerbatimTag => {
                    self.mode = GuardMode::ExpectNode;
                    self.plain_parent_indentation = None;
                }
            }
        } else if self.mode == GuardMode::Tag {
            self.mode = GuardMode::ExpectNode;
        }
        Ok(())
    }

    fn scan_quoted_character(
        &mut self,
        character: char,
        characters: &mut YamlCharacters<'_>,
        next_column: &mut usize,
    ) -> bool {
        match self.mode {
            GuardMode::SingleQuoted => {
                if character == '\'' {
                    if characters.peek().is_some_and(|(_, next)| *next == '\'') {
                        characters.next();
                        *next_column = next_column.saturating_add(1);
                    } else {
                        self.mode = GuardMode::AfterNode;
                    }
                }
                true
            }
            GuardMode::DoubleQuoted => {
                if character == '\\' {
                    if characters.next().is_some() {
                        *next_column = next_column.saturating_add(1);
                    }
                } else if character == '"' {
                    self.mode = GuardMode::AfterNode;
                }
                true
            }
            _ => false,
        }
    }

    fn scan_tag_character(&mut self, character: char, line_number: usize) -> ConfigResult<bool> {
        match self.mode {
            GuardMode::VerbatimTag => {
                if character == '>' {
                    self.mode = GuardMode::Tag;
                }
                Ok(true)
            }
            GuardMode::Tag => {
                if is_yaml_blank(character) {
                    self.mode = GuardMode::ExpectNode;
                } else if !self.flow_stack.is_empty() && matches!(character, ',' | ']' | '}') {
                    self.node_start_column = self
                        .node_property_start_column
                        .take()
                        .unwrap_or(self.node_start_column);
                    self.pending_parent_indentation = None;
                    self.mode = GuardMode::AfterNode;
                    self.scan_flow_delimiter(character, line_number)?;
                }
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn scan_plain_character(
        &mut self,
        character: char,
        next: Option<char>,
        column: usize,
        line_number: usize,
        line_parent_indentation: &mut usize,
    ) -> ConfigResult<()> {
        match character {
            ':' if is_plain_mapping_separator(next, !self.flow_stack.is_empty()) => {
                self.mode = GuardMode::ExpectNode;
                self.plain_parent_indentation = None;
                self.node_start_column = self.plain_start_column;
                *line_parent_indentation = self.plain_start_column;
                self.pending_parent_indentation = Some(self.plain_start_column);
                self.node_property_start_column = None;
            }
            ',' if !self.flow_stack.is_empty() => {
                self.mode = GuardMode::ExpectNode;
                self.plain_parent_indentation = None;
                self.pending_parent_indentation = None;
                self.node_property_start_column = None;
            }
            '[' | '{' if !self.flow_stack.is_empty() => {
                self.push_flow(character, column, line_number)?;
                self.mode = GuardMode::ExpectNode;
                self.plain_parent_indentation = None;
                self.pending_parent_indentation = None;
                self.node_property_start_column = None;
            }
            ']' | '}' if !self.flow_stack.is_empty() => {
                self.close_flow(character, line_number)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn scan_after_node_character(
        &mut self,
        character: char,
        column: usize,
        line_number: usize,
        line_parent_indentation: &mut usize,
    ) -> ConfigResult<()> {
        match character {
            character if is_yaml_blank(character) => {}
            ':' => {
                self.mode = GuardMode::ExpectNode;
                *line_parent_indentation = self.node_start_column;
                self.pending_parent_indentation = Some(self.node_start_column);
                self.node_property_start_column = None;
            }
            ',' if !self.flow_stack.is_empty() => {
                self.mode = GuardMode::ExpectNode;
                self.pending_parent_indentation = None;
                self.node_property_start_column = None;
            }
            ']' | '}' if !self.flow_stack.is_empty() => {
                self.close_flow(character, line_number)?;
            }
            '&' | '*' => return Err(control_token_error(character, line_number)),
            '[' | '{' if !self.flow_stack.is_empty() => {
                self.push_flow(character, column, line_number)?;
                self.mode = GuardMode::ExpectNode;
                self.pending_parent_indentation = None;
                self.node_property_start_column = None;
            }
            _ => {}
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn scan_expected_character(
        &mut self,
        line: &str,
        byte_index: usize,
        character: char,
        column: usize,
        line_number: usize,
        characters: &mut YamlCharacters<'_>,
        next_column: &mut usize,
        is_logical_column_zero: bool,
        line_parent_indentation: &mut usize,
    ) -> ConfigResult<bool> {
        let next = characters.peek().map(|(_, next)| *next);
        match character {
            character if is_yaml_blank(character) => {}
            '%' if self.flow_stack.is_empty() && is_logical_column_zero => {
                return Ok(true);
            }
            '-' | '.'
                if self.flow_stack.is_empty()
                    && is_logical_column_zero
                    && is_yaml_document_marker(line, byte_index, character) =>
            {
                characters.next();
                characters.next();
                *next_column = next_column.saturating_add(2);
            }
            '\'' => {
                self.mode = GuardMode::SingleQuoted;
                self.node_start_column = self.begin_node(column);
            }
            '"' => {
                self.mode = GuardMode::DoubleQuoted;
                self.node_start_column = self.begin_node(column);
            }
            '&' | '*' => return Err(control_token_error(character, line_number)),
            '!' => {
                let property_start = self.node_property_start_column.get_or_insert(column);
                self.node_start_column = *property_start;
                self.mode = if next == Some('<') {
                    GuardMode::VerbatimTag
                } else {
                    GuardMode::Tag
                };
            }
            '[' | '{' => {
                let node_start = self.begin_node(column);
                self.push_flow(character, node_start, line_number)?;
                self.mode = GuardMode::ExpectNode;
            }
            ']' | '}' if !self.flow_stack.is_empty() => {
                self.close_flow(character, line_number)?;
            }
            ',' if !self.flow_stack.is_empty() => {
                self.pending_parent_indentation = None;
                self.node_property_start_column = None;
            }
            ':' if is_plain_mapping_separator(next, !self.flow_stack.is_empty()) => {
                let key_start = self
                    .node_property_start_column
                    .take()
                    .unwrap_or(*line_parent_indentation);
                self.node_start_column = key_start;
                *line_parent_indentation = key_start;
                self.pending_parent_indentation = Some(key_start);
            }
            '-' | '?' if next.is_none_or(is_yaml_blank) => {
                if self.flow_stack.is_empty() {
                    *line_parent_indentation = column;
                    self.pending_parent_indentation = Some(column);
                    self.node_property_start_column = None;
                }
            }
            '|' | '>' if self.flow_stack.is_empty() => {
                if let Some(indentation) =
                    parse_block_scalar_suffix(&line[byte_index + character.len_utf8()..])
                {
                    let node_start = self.begin_node(column);
                    self.block_scalar =
                        Some(block_scalar_state(*line_parent_indentation, indentation));
                    self.mode = GuardMode::AfterNode;
                    self.node_start_column = node_start;
                    return Ok(true);
                }
                self.start_plain(column, *line_parent_indentation);
            }
            _ => self.start_plain(column, *line_parent_indentation),
        }
        Ok(false)
    }

    fn start_plain(&mut self, column: usize, parent_indentation: usize) {
        let allows_equal_indentation = self.pending_parent_indentation.is_none();
        let node_start = self.begin_node(column);
        self.mode = GuardMode::Plain;
        self.plain_start_column = node_start;
        self.node_start_column = node_start;
        self.plain_parent_indentation = if self.flow_stack.is_empty() {
            Some(parent_indentation)
        } else {
            None
        };
        self.plain_allows_equal_indentation =
            self.flow_stack.is_empty() && allows_equal_indentation;
    }

    fn begin_node(&mut self, content_column: usize) -> usize {
        self.pending_parent_indentation = None;
        self.node_property_start_column
            .take()
            .unwrap_or(content_column)
    }

    fn push_flow(&mut self, opening: char, column: usize, line_number: usize) -> ConfigResult<()> {
        if self.flow_stack.len() >= MAX_YAML_NESTING {
            return Err(ConfigError::Validation(format!(
                "YAML flow nesting exceeds {MAX_YAML_NESTING} levels in bounded configuration input (line {line_number})"
            )));
        }
        let closing = if opening == '[' { ']' } else { '}' };
        self.flow_stack.push(FlowFrame {
            closing,
            start_column: column,
        });
        Ok(())
    }

    fn close_flow(&mut self, closing: char, line_number: usize) -> ConfigResult<()> {
        let frame = self.flow_stack.pop().ok_or_else(|| {
            ConfigError::Validation(format!(
                "unexpected YAML flow delimiter {closing} in bounded configuration input (line {line_number})"
            ))
        })?;
        if frame.closing != closing {
            return Err(ConfigError::Validation(format!(
                "mismatched YAML flow delimiter {closing} in bounded configuration input (line {line_number})"
            )));
        }
        self.mode = GuardMode::AfterNode;
        self.node_start_column = frame.start_column;
        self.plain_parent_indentation = None;
        self.pending_parent_indentation = None;
        self.node_property_start_column = None;
        Ok(())
    }

    fn scan_flow_delimiter(&mut self, character: char, line_number: usize) -> ConfigResult<()> {
        match character {
            ',' => {
                self.mode = GuardMode::ExpectNode;
                self.pending_parent_indentation = None;
                self.node_property_start_column = None;
            }
            ']' | '}' => self.close_flow(character, line_number)?,
            _ => {}
        }
        Ok(())
    }
}

fn block_scalar_state(
    parent_indentation: usize,
    indentation: BlockScalarIndentation,
) -> BlockScalarState {
    BlockScalarState {
        parent_indentation,
        content_indentation: match indentation {
            BlockScalarIndentation::Implicit => None,
            BlockScalarIndentation::Explicit(value) => {
                Some(parent_indentation.saturating_add(value))
            }
        },
    }
}

fn control_token_error(character: char, line_number: usize) -> ConfigError {
    let token = if character == '&' { "anchor" } else { "alias" };
    ConfigError::Validation(format!(
        "YAML {token} tokens are not supported in bounded configuration input (line {line_number})"
    ))
}

fn is_plain_mapping_separator(next: Option<char>, in_flow: bool) -> bool {
    next.is_none_or(|character| {
        is_yaml_blank(character) || (in_flow && matches!(character, ',' | '[' | ']' | '{' | '}'))
    })
}

fn is_yaml_blank(character: char) -> bool {
    matches!(character, ' ' | '\t')
}

fn is_flow_boundary(character: char) -> bool {
    matches!(character, '[' | '{' | ',')
}

fn is_full_yaml_comment(line: &str) -> bool {
    line.trim_start_matches(is_yaml_blank).starts_with('#')
}

fn is_yaml_document_marker(line: &str, byte_index: usize, character: char) -> bool {
    let marker = if character == '-' { "---" } else { "..." };
    line[byte_index..].starts_with(marker)
        && line[byte_index + marker.len()..]
            .chars()
            .next()
            .is_none_or(|suffix| is_yaml_blank(suffix) || suffix == '#')
}

fn is_yaml_document_boundary_line(line: &str) -> bool {
    let line = line.trim_start_matches('\u{FEFF}');
    is_yaml_document_marker(line, 0, '-') || is_yaml_document_marker(line, 0, '.')
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockScalarIndentation {
    Implicit,
    Explicit(usize),
}

fn parse_block_scalar_suffix(suffix: &str) -> Option<BlockScalarIndentation> {
    let mut explicit_indentation = None;
    let mut chomping = None;
    let mut separated = false;
    for character in suffix.chars() {
        match character {
            '\r' | '\n' => break,
            ' ' | '\t' => separated = true,
            '#' if separated => break,
            '+' | '-' if !separated && chomping.is_none() => chomping = Some(character),
            '1'..='9' if !separated && explicit_indentation.is_none() => {
                explicit_indentation = character.to_digit(10).map(|value| value as usize);
            }
            _ => return None,
        }
    }
    Some(explicit_indentation.map_or(
        BlockScalarIndentation::Implicit,
        BlockScalarIndentation::Explicit,
    ))
}

fn is_blank_yaml_line(line: &str) -> bool {
    line.chars().all(is_yaml_blank)
}

fn yaml_line_indentation(line: &str) -> usize {
    line.chars()
        .skip_while(|character| *character == '\u{FEFF}')
        .take_while(|character| *character == ' ')
        .count()
}

#[cfg(test)]
mod tests {
    use super::{MAX_CONFIG_FILE_BYTES, parse_yaml, reject_yaml_anchors_and_aliases};
    use serde_yaml::Value;

    fn assert_guard_accepts_and_parses(yaml: &str) {
        reject_yaml_anchors_and_aliases(yaml).unwrap();
        serde_yaml::from_str::<Value>(yaml).unwrap();
    }

    fn assert_guard_rejects(yaml: &str, token: &str) {
        let error = reject_yaml_anchors_and_aliases(yaml).unwrap_err();
        assert!(
            error.to_string().contains(&format!("YAML {token} tokens")),
            "{error}"
        );
    }

    #[test]
    fn block_scalar_headers_keep_literal_tokens_out_of_control_scanning() {
        for header in [
            "|", ">", "|-", "|+", ">-", ">+", "|2", ">2", "|2-", "|-2", ">+2",
        ] {
            let yaml = format!("notes: {header}\n\n  *literal\n  &literal\n\nnext: plain\n");
            assert!(
                reject_yaml_anchors_and_aliases(&yaml).is_ok(),
                "header {header}"
            );
        }
    }

    #[test]
    fn nested_block_scalar_exits_at_its_parent_indentation() {
        let yaml = r"
- key: |2
    *literal
    &literal

  copy: *actual_alias
";

        let error = reject_yaml_anchors_and_aliases(yaml).unwrap_err();

        assert!(error.to_string().contains("YAML alias tokens"));
        assert!(error.to_string().contains("line 6"));
    }

    #[test]
    fn explicit_block_indentation_uses_the_structural_parent_of_complex_keys() {
        let legal_yaml = r"- ? key: |2
      literal &literal *literal
    sibling: value
  : outer
";
        assert_guard_accepts_and_parses(legal_yaml);

        let anchored_yaml = r"- ? key: |2
      literal
    sibling: &actual value
    copy: *actual
  : outer
";
        let error = reject_yaml_anchors_and_aliases(anchored_yaml).unwrap_err();
        assert!(error.to_string().contains("YAML anchor tokens"), "{error}");
        assert!(error.to_string().contains("line 3"), "{error}");
    }

    #[test]
    fn tag_only_mapping_keys_preserve_their_structural_parent() {
        for tag in ["!foo", "!!str", "!<tag:example.test,2026:kind>"] {
            let implicit = format!("- {tag} : |\n  sibling: &actual value\n  copy: *actual\n");
            serde_yaml::from_str::<Value>(&implicit).unwrap();
            assert_guard_rejects(&implicit, "anchor");

            let explicit =
                format!("- {tag} : |2\n    literal\n  sibling: &actual value\n  copy: *actual\n");
            serde_yaml::from_str::<Value>(&explicit).unwrap();
            assert_guard_rejects(&explicit, "anchor");
        }
    }

    #[test]
    fn tagged_mapping_keys_keep_literal_block_content_accepted() {
        for yaml in [
            "!foo \"key\": |2\n  &literal *literal\nnext: ok\n",
            "- !foo \"key\": |2\n    &literal *literal\n  next: ok\n",
            "!<tag:example.test,2026:kind> plain: |2\n  &literal *literal\nnext: ok\n",
        ] {
            assert_guard_accepts_and_parses(yaml);
        }
    }

    #[test]
    fn multiline_plain_nodes_inherit_their_structural_parent_across_lines() {
        for yaml in [
            "key:\n  text\n  &literal *literal\nnext: ok\n",
            "-\n  text\n  &literal *literal\n- next\n",
            "key: !foo\n  text\n  &literal *literal\nnext: ok\n",
            "? key\n:\n  text\n  &literal *literal\nnext: ok\n",
        ] {
            assert_guard_accepts_and_parses(yaml);
        }
    }

    #[test]
    fn root_plain_scalars_allow_equal_indentation_continuations() {
        for yaml in [
            "text\n&literal *literal\n",
            " text\n &literal *literal\n",
            "!foo text\n&literal *literal\n",
            "---\ntext\n&literal *literal\n",
        ] {
            assert_guard_accepts_and_parses(yaml);
        }
    }

    #[test]
    fn root_plain_continuations_stop_at_document_boundaries() {
        for (yaml, token) in [
            ("text\n---\n&actual value\n", "anchor"),
            ("text\n...\n---\n*actual\n", "alias"),
        ] {
            assert_guard_rejects(yaml, token);
        }
    }

    #[test]
    fn utf8_bom_handling_matches_each_public_parsing_surface() {
        let ordinary = "\u{FEFF}name: demo\nenabled: true\n";
        let parsed = parse_yaml::<Value>(ordinary).unwrap();
        assert_eq!(parsed["name"], Value::String("demo".to_owned()));
        assert_eq!(parsed["enabled"], Value::Bool(true));

        let raw_yaml = "\u{FEFF}notes: |1\n  literal\n base: &actual value\n copy: *actual\n";
        serde_yaml::from_str::<Value>(raw_yaml).unwrap();
        let error = reject_yaml_anchors_and_aliases(raw_yaml).unwrap_err();
        assert!(error.to_string().contains("YAML anchor tokens"), "{error}");
        assert!(error.to_string().contains("line 3"), "{error}");

        let normalized = parse_yaml::<Value>(raw_yaml).unwrap();
        assert!(normalized.get("base").is_none());
        assert!(
            normalized["notes"]
                .as_str()
                .is_some_and(|notes| notes.contains("base: &actual value"))
        );
    }

    #[test]
    fn block_scalar_dedent_restores_real_anchor_and_alias_rejection() {
        for control in ["copy: *actual_alias", "defaults: &actual_anchor"] {
            let yaml = format!("notes: |-\n\n  *literal\n  &literal\n\n{control}\n");
            let error = reject_yaml_anchors_and_aliases(&yaml).unwrap_err();
            assert!(error.to_string().contains("YAML"), "{error}");
            assert!(error.to_string().contains("tokens"), "{error}");
        }
    }

    #[test]
    fn quoted_globs_comments_and_plain_url_characters_remain_valid() {
        let yaml = r#"
double: "*_PERP"
single: '*SPOT*'
literal: "&not-an-anchor"
url: https://example.invalid/a&b
# * comment bullet
"#;

        assert!(reject_yaml_anchors_and_aliases(yaml).is_ok());
    }

    #[test]
    fn bare_carriage_returns_end_block_scalars_and_comments() {
        let block_yaml = "notes: |-\r  *literal\r  &literal\rcopy: *actual_alias\r";
        let block_error = reject_yaml_anchors_and_aliases(block_yaml).unwrap_err();
        assert!(block_error.to_string().contains("YAML alias tokens"));
        assert!(block_error.to_string().contains("line 4"));

        let comment_yaml = "safe: value # *comment\rdefaults: &actual_anchor\r";
        let comment_error = reject_yaml_anchors_and_aliases(comment_yaml).unwrap_err();
        assert!(comment_error.to_string().contains("YAML anchor tokens"));
        assert!(comment_error.to_string().contains("line 2"));
    }

    #[test]
    fn unicode_yaml_line_separators_do_not_hide_aliases() {
        for separator in ['\u{0085}', '\u{2028}', '\u{2029}'] {
            let yaml = format!("safe: value # *comment{separator}copy: *actual_alias");
            let error = reject_yaml_anchors_and_aliases(&yaml).unwrap_err();
            assert!(
                error.to_string().contains("YAML alias tokens"),
                "separator U+{:04X}: {error}",
                u32::from(separator)
            );
            assert!(error.to_string().contains("line 2"), "{error}");
        }
    }

    #[test]
    fn every_yaml_line_separator_preserves_block_scalar_literals() {
        for separator in ['\n', '\r', '\u{0085}', '\u{2028}', '\u{2029}'] {
            let yaml = format!(
                "notes: |-{separator}  *literal{separator}  &literal{separator}next: plain{separator}"
            );
            assert!(
                reject_yaml_anchors_and_aliases(&yaml).is_ok(),
                "separator U+{:04X}",
                u32::from(separator)
            );
        }
    }

    #[test]
    fn carriage_return_line_feed_counts_as_one_logical_line() {
        let error =
            reject_yaml_anchors_and_aliases("safe: value # *comment\r\ncopy: *actual_alias\r\n")
                .unwrap_err();

        assert!(error.to_string().contains("YAML alias tokens"));
        assert!(error.to_string().contains("line 2"));
    }

    #[test]
    fn column_zero_byte_order_marks_cannot_hide_anchor_or_alias_nodes() {
        for (yaml, token) in [
            ("\u{FEFF}&flow_anchor [one, two]\n", "anchor"),
            ("\u{FEFF}*flow_alias\n", "alias"),
            ("\u{FEFF}\u{FEFF}&double_bom_anchor value\n", "anchor"),
            ("\u{FEFF}\u{FEFF}*double_bom_alias\n", "alias"),
        ] {
            let error = reject_yaml_anchors_and_aliases(yaml).unwrap_err();
            assert!(
                error.to_string().contains(&format!("YAML {token} tokens")),
                "{error}"
            );
            assert!(error.to_string().contains("line 1"), "{error}");
        }
    }

    #[test]
    fn byte_order_marks_remain_literal_inside_blocks_quotes_and_plain_content() {
        let block = "notes: |-\n  \u{FEFF}&literal\n  \u{FEFF}*literal\nnext: plain\n";
        let quoted = "\u{FEFF}value: \"\u{FEFF}&literal *literal\"\n";
        let middle = "value: prefix\u{FEFF}&literal\n";

        assert!(reject_yaml_anchors_and_aliases(block).is_ok());
        assert!(reject_yaml_anchors_and_aliases(quoted).is_ok());
        assert!(reject_yaml_anchors_and_aliases(middle).is_ok());
    }

    #[test]
    fn quotes_inside_plain_mapping_keys_do_not_hide_control_tokens() {
        for quote in ['\'', '"'] {
            let anchor_yaml = format!("plain{quote}key: &actual value\n");
            let anchor_error = reject_yaml_anchors_and_aliases(&anchor_yaml).unwrap_err();
            assert!(
                anchor_error.to_string().contains("YAML anchor tokens"),
                "{anchor_error}"
            );

            let alias_yaml = format!("plain{quote}key: value\ncopy: *actual\n");
            let alias_error = reject_yaml_anchors_and_aliases(&alias_yaml).unwrap_err();
            assert!(
                alias_error.to_string().contains("YAML alias tokens"),
                "{alias_error}"
            );
            assert!(alias_error.to_string().contains("line 2"));
        }
    }

    #[test]
    fn quotes_inside_plain_flow_scalars_do_not_hide_control_tokens() {
        for quote in ['\'', '"'] {
            let anchor_yaml = format!("[plain{quote}quote, &actual value]\n");
            let anchor_error = reject_yaml_anchors_and_aliases(&anchor_yaml).unwrap_err();
            assert!(anchor_error.to_string().contains("YAML anchor tokens"));

            let alias_yaml = format!("[plain{quote}quote, value, *actual]\n");
            let alias_error = reject_yaml_anchors_and_aliases(&alias_yaml).unwrap_err();
            assert!(alias_error.to_string().contains("YAML alias tokens"));
        }
    }

    #[test]
    fn whitespace_before_plain_quotes_does_not_start_quoted_state() {
        for quote in ['\'', '"'] {
            let yaml = format!("plain key {quote}still plain: &actual value\n");
            let error = reject_yaml_anchors_and_aliases(&yaml).unwrap_err();
            assert!(error.to_string().contains("YAML anchor tokens"), "{error}");
        }
    }

    #[test]
    fn tags_and_document_markers_restore_token_start_state() {
        for yaml in [
            "!custom &actual value\n",
            "!<tag:example.test,2026:kind> &actual value\n",
            "--- &actual value\n",
        ] {
            let error = reject_yaml_anchors_and_aliases(yaml).unwrap_err();
            assert!(error.to_string().contains("YAML anchor tokens"), "{error}");
        }
    }

    #[test]
    fn real_quoted_and_block_scalars_keep_literal_indicators() {
        let quoted = r#"
double: "literal &anchor *alias"
single: 'literal &anchor *alias'
"#;
        let block = "notes: |-\n  plain'quote &literal\n  plain\"quote *literal\nnext: ok\n";

        assert!(reject_yaml_anchors_and_aliases(quoted).is_ok());
        assert!(reject_yaml_anchors_and_aliases(block).is_ok());
    }

    #[test]
    fn multiline_plain_quotes_cannot_mask_later_control_nodes() {
        for quote in ['\'', '"'] {
            let anchor_yaml =
                format!("first: line one\n  {quote}still plain\nsecond: &actual value\n");
            assert_guard_rejects(&anchor_yaml, "anchor");

            let alias_yaml = format!("first: line one\n  {quote}still plain\nsecond: *actual\n");
            assert_guard_rejects(&alias_yaml, "alias");

            let legal_yaml =
                format!("first: line one\n  {quote}still plain &literal *literal\nsecond: ok\n");
            assert_guard_accepts_and_parses(&legal_yaml);
        }
    }

    #[test]
    fn multiline_flow_plain_quotes_cannot_mask_control_nodes() {
        for quote in ['\'', '"'] {
            let anchor_yaml = format!("[line one\n  {quote}still plain, &actual value]\n");
            assert_guard_rejects(&anchor_yaml, "anchor");

            let alias_yaml = format!("[line one\n  {quote}still plain, *actual]\n");
            assert_guard_rejects(&alias_yaml, "alias");

            let legal_yaml = format!("[line one\n  {quote}still plain &literal *literal, next]\n");
            assert_guard_accepts_and_parses(&legal_yaml);
        }
    }

    #[test]
    fn block_plain_dedent_and_sequence_siblings_restore_node_scanning() {
        let legal_root = "first: value\n  continuation &literal *literal\nsecond: plain\n";
        assert_guard_accepts_and_parses(legal_root);

        let alias_after_dedent =
            "first: value\n  continuation &literal *literal\nsecond: *actual\n";
        assert_guard_rejects(alias_after_dedent, "alias");

        let legal_sequence = "- key: first\n    continuation &literal *literal\n  sibling: plain\n";
        assert_guard_accepts_and_parses(legal_sequence);

        let anchor_in_sibling =
            "- key: first\n    continuation &literal *literal\n  sibling: &actual value\n";
        assert_guard_rejects(anchor_in_sibling, "anchor");
    }

    #[test]
    fn flow_colon_context_distinguishes_plain_text_from_control_nodes() {
        for yaml in ["{plain:&literal}\n", "{plain:\"literal\"}\n"] {
            assert_guard_accepts_and_parses(yaml);
        }

        assert_guard_rejects("{\"key\":&actual value}\n", "anchor");
        assert_guard_rejects("{key:[&actual value]}\n", "anchor");
        assert_guard_rejects("{\"key\":*actual}\n", "alias");
    }

    #[test]
    fn tags_directives_and_document_boundaries_do_not_mask_control_nodes() {
        for yaml in [
            "!custom &actual value\n",
            "!<tag:example.test,2026:kind&literal> &actual value\n",
            "--- &actual value\n",
            "...\n---\n&actual value\n",
        ] {
            assert_guard_rejects(yaml, "anchor");
        }

        let directive = "%TAG !e! tag:example.test,2026:&literal\n---\nvalue: plain\n";
        reject_yaml_anchors_and_aliases(directive).unwrap();

        let indented_marker = "first: value\n  --- &literal *literal\nsecond: plain\n";
        assert_guard_accepts_and_parses(indented_marker);
    }

    #[test]
    fn comments_and_plain_colons_keep_literal_indicators_literal() {
        let yaml = r"
url: https://example.invalid/a&b*c
hash: foo#bar&baz*qux
comment: value # &comment *comment
flow: [https://example.invalid/a&b, foo#bar*qux]
";
        assert_guard_accepts_and_parses(yaml);

        assert_guard_rejects("key: &actual value\n", "anchor");
        assert_guard_rejects("key: *actual\n", "alias");
    }

    #[test]
    fn every_control_indicator_at_node_start_fails_closed() {
        for anchor in ["&", "&:", "&!", "&#", "&锚点", "&-name"] {
            assert_guard_rejects(&format!("value: {anchor}\n"), "anchor");
        }
        for alias in ["*", "*:", "*!", "*#", "*别名", "*-name"] {
            assert_guard_rejects(&format!("value: {alias}\n"), "alias");
        }
    }

    #[test]
    fn tagged_block_scalars_keep_literal_indicators_inside_content() {
        let yaml = "value: !custom |-\n  &literal *literal\nnext: plain\n";
        assert_guard_accepts_and_parses(yaml);

        let anchor_after_dedent = "value: !custom |-\n  &literal *literal\nnext: &actual value\n";
        assert_guard_rejects(anchor_after_dedent, "anchor");
    }

    #[test]
    fn excessive_flow_nesting_is_rejected_before_deserialization() {
        let yaml = format!("{}value{}", "[".repeat(129), "]".repeat(129));
        let error = reject_yaml_anchors_and_aliases(&yaml).unwrap_err();
        assert!(error.to_string().contains("exceeds 128 levels"), "{error}");
    }

    #[test]
    fn maximum_size_single_line_scans_in_one_forward_pass() {
        let prefix = "value: ";
        let yaml = format!(
            "{prefix}{}\n",
            "x".repeat(MAX_CONFIG_FILE_BYTES - prefix.len() - 1)
        );
        assert_eq!(yaml.len(), MAX_CONFIG_FILE_BYTES);

        reject_yaml_anchors_and_aliases(&yaml).unwrap();
    }
}
