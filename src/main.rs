

use gerc::importar_deudores;
use anyhow::Result;


fn main() -> Result<()> {
    importar_deudores::run()?;
    Ok(())
}

